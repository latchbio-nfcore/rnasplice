/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    VALIDATE INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

def summary_params = NfcoreSchema.paramsSummaryMap(workflow, params)

// Validate input parameters
WorkflowRnasplice.initialise(params, log)

// TODO nf-core: Add all file path parameters for the pipeline to the list below
// Check input path parameters to see if they exist
def checkPathParamList = [ params.input, params.multiqc_config, params.fasta ]
for (param in checkPathParamList) { if (param) { file(param, checkIfExists: true) } }

// Check mandatory parameters
if (params.input) { ch_input = file(params.input) } else { exit 1, 'Input samplesheet not specified!' }

// Check alignment parameters
def prepareToolIndices  = []
if (!params.skip_alignment) { prepareToolIndices << params.aligner        }
if (params.pseudo_aligner)  { prepareToolIndices << params.pseudo_aligner }

// Check if an AWS iGenome has been provided to use the appropriate version of STAR
def is_aws_igenome = false

if (params.fasta && params.gtf) {

    if ((file(params.fasta).getName() - '.gz' == 'genome.fa') && (file(params.gtf).getName() - '.gz' == 'genes.gtf')) {
        is_aws_igenome = true
    }   
}
// Stage dummy file to be used as an optional input where required
ch_dummy_file = file("$projectDir/assets/dummy_file.txt", checkIfExists: true)

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CONFIG FILES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

ch_multiqc_config        = file("$projectDir/assets/multiqc_config.yml", checkIfExists: true)
ch_multiqc_custom_config = params.multiqc_config ? Channel.fromPath(params.multiqc_config) : Channel.empty()

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// SUBWORKFLOW: Consisting of a mix of local and nf-core/modules
//
include { INPUT_CHECK       } from '../subworkflows/local/input_check'
include { PREPARE_GENOME    } from '../subworkflows/local/prepare_genome'
include { FASTQC_TRIMGALORE } from '../subworkflows/local/fastqc_trimgalore'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT NF-CORE MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// MODULE: Installed directly from nf-core/modules
//
include { SALMON_QUANT                      } from '../modules/nf-core/modules/salmon/quant/main'
include { SALMON_QUANT as STAR_SALMON_QUANT } from '../modules/nf-core/modules/salmon/quant/main'
include { STAR_ALIGN                        } from '../modules/nf-core/modules/star/align/main'
include { MULTIQC                           } from '../modules/nf-core/modules/multiqc/main'
include { CUSTOM_DUMPSOFTWAREVERSIONS       } from '../modules/nf-core/modules/custom/dumpsoftwareversions/main'

//
// SUBWORKFLOWS: Installed directly from nf-core/modules
//
include { BAM_SORT_SAMTOOLS } from '../subworkflows/nf-core/bam_sort_samtools'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Info required for completion email and summary
def multiqc_report = []

workflow RNASPLICE {

    // Create channel for software versions (will be added to throughout pipeline)
    ch_versions = Channel.empty()

    //
    // SUBWORKFLOW: Uncompress and prepare reference genome files
    //

    def biotype = params.gencode ? "gene_type" : params.featurecounts_group_type

    PREPARE_GENOME (
        prepareToolIndices,
        biotype,
        is_aws_igenome
    )

    ch_versions = ch_versions.mix(PREPARE_GENOME.out.versions)

    //
    // SUBWORKFLOW: Read in samplesheet, validate and stage input files
    //

    // Run Input check subworkflow
    INPUT_CHECK ( ch_input )

    // Take software versions from input check (.first() not required)
    ch_versions = ch_versions.mix(INPUT_CHECK.out.versions)

    //
    // SUBWORKFLOW: Read QC, and trimming
    //

    // Run FastQC and TrimGalore
    FASTQC_TRIMGALORE (
        INPUT_CHECK.out.reads,
        params.skip_fastqc || params.skip_qc,
        params.skip_trimming
    )
    
    // Take software versions from subworkflow (.first() not required)
    ch_versions = ch_versions.mix(FASTQC_TRIMGALORE.out.versions)

    // Collect trimmed reads from Trimgalore
    ch_trim_reads = FASTQC_TRIMGALORE.out.reads

    //
    // MODULE: Align reads using STAR
    //
            
    // Align with STAR
    if (!params.skip_alignment && (params.aligner == 'star_salmon' || params.aligner == "star")) {
        
        // Run Star alignment module 
        STAR_ALIGN ( 
            ch_trim_reads, 
            PREPARE_GENOME.out.star_index, 
            PREPARE_GENOME.out.gtf, 
            params.star_ignore_sjdbgtf, 
            params.seq_platform ?: '', 
            params.seq_center ?: ''
        )
        // Collect STAR output 
        ch_orig_bam          = STAR_ALIGN.out.bam            // channel: [ val(meta), bam            ]
        ch_log_final         = STAR_ALIGN.out.log_final      // channel: [ val(meta), log_final      ]
        ch_log_out           = STAR_ALIGN.out.log_out        // channel: [ val(meta), log_out        ]
        ch_log_progress      = STAR_ALIGN.out.log_progress   // channel: [ val(meta), log_progress   ]
        ch_bam_sorted        = STAR_ALIGN.out.bam_sorted     // channel: [ val(meta), bam_sorted     ]
        ch_transcriptome_bam = STAR_ALIGN.out.bam_transcript // channel: [ val(meta), bam_transcript ]
        ch_fastq             = STAR_ALIGN.out.fastq          // channel: [ val(meta), fastq          ]   
        ch_tab               = STAR_ALIGN.out.tab            // channel: [ val(meta), tab            ]

        // Collect software version
        ch_versions       = ch_versions.mix(STAR_ALIGN.out.versions.first())

        //
        // SUBWORKFLOW: Sort, index BAM file and run samtools stats, flagstat and idxstats
        //

        // Run Samtools subworkflow (sort, index with stats)
        BAM_SORT_SAMTOOLS ( ch_orig_bam )

        // Collect Samtools output - sorted bam, indices (bai, csi)
        ch_genome_bam        = BAM_SORT_SAMTOOLS.out.bam         // channel: [ val(meta), [ bam ] ]
        ch_genome_bam_index  = BAM_SORT_SAMTOOLS.out.bai         // channel: [ val(meta), [ bai ] ]
        ch_genom_csi         = BAM_SORT_SAMTOOLS.out.csi         // channel: [ val(meta), [ csi ] ]

        // Collect Samtools stats output - stats, flagstat, idxstats
        ch_samtools_stats    = BAM_SORT_SAMTOOLS.out.stats       // channel: [ val(meta), [ stats ] ]
        ch_samtools_flagstat = BAM_SORT_SAMTOOLS.out.flagstat    // channel: [ val(meta), [ flagstat ] ]
        ch_samtools_idxstats = BAM_SORT_SAMTOOLS.out.idxstats    // channel: [ val(meta), [ idxstats ] ]
        
        // Collect software version
        ch_versions = ch_versions.mix(BAM_SORT_SAMTOOLS.out.versions)

        //
        // SUBWORKFLOW: Count reads from BAM alignments using Salmon
        //

        // If needed run Salmon on transcriptome bam
        if (!params.skip_alignment && params.aligner == 'star_salmon') {

            alignment_mode = true
            ch_salmon_index = ch_dummy_file

            // Run Salmon quant, Run tx2gene.py (tx2gene for Salmon txImport Quantification), then finally runs tximport 
            STAR_SALMON_QUANT (
                ch_transcriptome_bam,
                ch_salmon_index,
                PREPARE_GENOME.out.gtf,
                PREPARE_GENOME.out.transcript_fasta,
                alignment_mode,
                params.salmon_quant_libtype ?: ''
            )

            ch_versions = ch_versions.mix(STAR_SALMON_QUANT.out.versions)
        
        }

    }

    //
    // SUBWORKFLOW: Pseudo-alignment and quantification with Salmon
    //

    if (params.pseudo_aligner == 'salmon') {
        
        alignment_mode = false
        ch_transcript_fasta = ch_dummy_file

        SALMON_QUANT (
            ch_trim_reads,
            PREPARE_GENOME.out.salmon_index,
            PREPARE_GENOME.out.gtf,
            ch_transcript_fasta,
            alignment_mode,
            params.salmon_quant_libtype ?: ''
        )

        // Collect Salmon quant output
        ch_salmon_multiqc = SALMON_QUANT.out.results

        // Take software versions from subworkflow (.first() not required)
        ch_versions = ch_versions.mix(SALMON_QUANT.out.versions)
    }
    
    //
    // MODULE: Collect version information across pipeline
    //

    CUSTOM_DUMPSOFTWAREVERSIONS (
        ch_versions.unique().collectFile(name: 'collated_versions.yml')
    )

    //
    // MODULE: MultiQC
    //

    workflow_summary    = WorkflowRnasplice.paramsSummaryMultiqc(workflow, summary_params)
    ch_workflow_summary = Channel.value(workflow_summary)

    ch_multiqc_files = Channel.empty()
    ch_multiqc_files = ch_multiqc_files.mix(Channel.from(ch_multiqc_config))
    ch_multiqc_files = ch_multiqc_files.mix(ch_multiqc_custom_config.collect().ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(ch_workflow_summary.collectFile(name: 'workflow_summary_mqc.yaml'))
    ch_multiqc_files = ch_multiqc_files.mix(CUSTOM_DUMPSOFTWAREVERSIONS.out.mqc_yml.collect())
    ch_multiqc_files = ch_multiqc_files.mix(FASTQC_TRIMGALORE.out.fastqc_zip.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(FASTQC_TRIMGALORE.out.trim_zip.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(FASTQC_TRIMGALORE.out.trim_log.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(ch_salmon_multiqc.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(ch_log_final.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(ch_samtools_stats.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(ch_samtools_flagstat.collect{it[1]}.ifEmpty([]))
    ch_multiqc_files = ch_multiqc_files.mix(ch_samtools_idxstats.collect{it[1]}.ifEmpty([]))

    MULTIQC (
        ch_multiqc_files.collect()
    )
    multiqc_report = MULTIQC.out.report.toList()
    ch_versions    = ch_versions.mix(MULTIQC.out.versions)
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    COMPLETION EMAIL AND SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow.onComplete {
    if (params.email || params.email_on_fail) {
        NfcoreTemplate.email(workflow, params, summary_params, projectDir, log, multiqc_report)
    }
    NfcoreTemplate.summary(workflow, params, log)
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/