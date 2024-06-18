from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        }
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]






@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(pvc_name: str, input: LatchFile, contrasts: str, outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], multiqc_title: typing.Optional[str], save_merged_fastq: typing.Optional[bool], genome: typing.Optional[str], fasta: typing.Optional[LatchFile], gtf: typing.Optional[LatchFile], gff: typing.Optional[LatchFile], transcript_fasta: typing.Optional[LatchFile], star_index: typing.Optional[str], salmon_index: typing.Optional[str], gencode: typing.Optional[bool], save_reference: typing.Optional[bool], multiqc_methods_description: typing.Optional[str], clip_r1: typing.Optional[int], clip_r2: typing.Optional[int], three_prime_clip_r1: typing.Optional[int], three_prime_clip_r2: typing.Optional[int], trim_nextseq: typing.Optional[int], save_trimmed: typing.Optional[bool], skip_trimming: typing.Optional[bool], skip_trimgalore_fastqc: typing.Optional[bool], skip_fastqc: typing.Optional[bool], bam_csi_index: typing.Optional[bool], star_ignore_sjdbgtf: typing.Optional[bool], salmon_quant_libtype: typing.Optional[str], seq_center: typing.Optional[str], skip_alignment: typing.Optional[bool], save_unaligned: typing.Optional[bool], save_align_intermeds: typing.Optional[bool], rmats: typing.Optional[bool], rmats_novel_splice_site: typing.Optional[bool], dexseq_exon: typing.Optional[bool], save_dexseq_annotation: typing.Optional[bool], gff_dexseq: typing.Optional[LatchFile], edger_exon: typing.Optional[bool], dexseq_dtu: typing.Optional[bool], sashimi_plot: typing.Optional[bool], suppa_tpm: typing.Optional[str], diffsplice_median: typing.Optional[bool], clusterevents_sigthreshold: typing.Optional[float], clusterevents_separation: typing.Optional[int], source: str, gtf_extra_attributes: typing.Optional[str], gtf_group_features: typing.Optional[str], min_trimmed_reads: typing.Optional[int], skip_bigwig: typing.Optional[bool], aligner: typing.Optional[str], pseudo_aligner: typing.Optional[str], rmats_splice_diff_cutoff: typing.Optional[float], rmats_paired_stats: typing.Optional[bool], rmats_read_len: typing.Optional[int], rmats_min_intron_len: typing.Optional[int], rmats_max_exon_len: typing.Optional[int], alignment_quality: typing.Optional[int], aggregation: typing.Optional[bool], save_dexseq_plot: typing.Optional[bool], n_dexseq_plot: typing.Optional[int], save_edger_plot: typing.Optional[bool], n_edger_plot: typing.Optional[int], dtu_txi: typing.Optional[str], min_samps_gene_expr: typing.Optional[int], min_samps_feature_expr: typing.Optional[int], min_samps_feature_prop: typing.Optional[int], min_gene_expr: typing.Optional[int], min_feature_expr: typing.Optional[int], min_feature_prop: typing.Optional[float], miso_genes: typing.Optional[str], miso_genes_file: typing.Optional[str], miso_read_len: typing.Optional[int], fig_height: typing.Optional[int], fig_width: typing.Optional[int], suppa: typing.Optional[bool], suppa_per_local_event: typing.Optional[bool], suppa_per_isoform: typing.Optional[bool], generateevents_pool_genes: typing.Optional[bool], generateevents_event_type: typing.Optional[str], generateevents_boundary: typing.Optional[str], generateevents_threshold: typing.Optional[int], generateevents_exon_length: typing.Optional[int], psiperevent_total_filter: typing.Optional[int], diffsplice_local_event: typing.Optional[bool], diffsplice_isoform: typing.Optional[bool], diffsplice_method: typing.Optional[str], diffsplice_area: typing.Optional[int], diffsplice_lower_bound: typing.Optional[int], diffsplice_gene_correction: typing.Optional[bool], diffsplice_paired: typing.Optional[bool], diffsplice_alpha: typing.Optional[float], diffsplice_tpm_threshold: typing.Optional[int], diffsplice_nan_threshold: typing.Optional[int], clusterevents_local_event: typing.Optional[bool], clusterevents_isoform: typing.Optional[bool], clusterevents_dpsithreshold: typing.Optional[float], clusterevents_eps: typing.Optional[float], clusterevents_metric: typing.Optional[str], clusterevents_min_pts: typing.Optional[int], clusterevents_method: typing.Optional[str]) -> None:
    try:
        shared_dir = Path("/nf-workdir")



        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
                *get_flag('input', input),
                *get_flag('contrasts', contrasts),
                *get_flag('source', source),
                *get_flag('outdir', outdir),
                *get_flag('email', email),
                *get_flag('multiqc_title', multiqc_title),
                *get_flag('save_merged_fastq', save_merged_fastq),
                *get_flag('genome', genome),
                *get_flag('fasta', fasta),
                *get_flag('gtf', gtf),
                *get_flag('gff', gff),
                *get_flag('transcript_fasta', transcript_fasta),
                *get_flag('star_index', star_index),
                *get_flag('salmon_index', salmon_index),
                *get_flag('gencode', gencode),
                *get_flag('gtf_extra_attributes', gtf_extra_attributes),
                *get_flag('gtf_group_features', gtf_group_features),
                *get_flag('save_reference', save_reference),
                *get_flag('multiqc_methods_description', multiqc_methods_description),
                *get_flag('clip_r1', clip_r1),
                *get_flag('clip_r2', clip_r2),
                *get_flag('three_prime_clip_r1', three_prime_clip_r1),
                *get_flag('three_prime_clip_r2', three_prime_clip_r2),
                *get_flag('trim_nextseq', trim_nextseq),
                *get_flag('save_trimmed', save_trimmed),
                *get_flag('skip_trimming', skip_trimming),
                *get_flag('skip_trimgalore_fastqc', skip_trimgalore_fastqc),
                *get_flag('min_trimmed_reads', min_trimmed_reads),
                *get_flag('skip_fastqc', skip_fastqc),
                *get_flag('skip_bigwig', skip_bigwig),
                *get_flag('aligner', aligner),
                *get_flag('pseudo_aligner', pseudo_aligner),
                *get_flag('bam_csi_index', bam_csi_index),
                *get_flag('star_ignore_sjdbgtf', star_ignore_sjdbgtf),
                *get_flag('salmon_quant_libtype', salmon_quant_libtype),
                *get_flag('seq_center', seq_center),
                *get_flag('skip_alignment', skip_alignment),
                *get_flag('save_unaligned', save_unaligned),
                *get_flag('save_align_intermeds', save_align_intermeds),
                *get_flag('rmats', rmats),
                *get_flag('rmats_splice_diff_cutoff', rmats_splice_diff_cutoff),
                *get_flag('rmats_paired_stats', rmats_paired_stats),
                *get_flag('rmats_read_len', rmats_read_len),
                *get_flag('rmats_novel_splice_site', rmats_novel_splice_site),
                *get_flag('rmats_min_intron_len', rmats_min_intron_len),
                *get_flag('rmats_max_exon_len', rmats_max_exon_len),
                *get_flag('dexseq_exon', dexseq_exon),
                *get_flag('save_dexseq_annotation', save_dexseq_annotation),
                *get_flag('gff_dexseq', gff_dexseq),
                *get_flag('alignment_quality', alignment_quality),
                *get_flag('aggregation', aggregation),
                *get_flag('save_dexseq_plot', save_dexseq_plot),
                *get_flag('n_dexseq_plot', n_dexseq_plot),
                *get_flag('edger_exon', edger_exon),
                *get_flag('save_edger_plot', save_edger_plot),
                *get_flag('n_edger_plot', n_edger_plot),
                *get_flag('dexseq_dtu', dexseq_dtu),
                *get_flag('dtu_txi', dtu_txi),
                *get_flag('min_samps_gene_expr', min_samps_gene_expr),
                *get_flag('min_samps_feature_expr', min_samps_feature_expr),
                *get_flag('min_samps_feature_prop', min_samps_feature_prop),
                *get_flag('min_gene_expr', min_gene_expr),
                *get_flag('min_feature_expr', min_feature_expr),
                *get_flag('min_feature_prop', min_feature_prop),
                *get_flag('sashimi_plot', sashimi_plot),
                *get_flag('miso_genes', miso_genes),
                *get_flag('miso_genes_file', miso_genes_file),
                *get_flag('miso_read_len', miso_read_len),
                *get_flag('fig_height', fig_height),
                *get_flag('fig_width', fig_width),
                *get_flag('suppa', suppa),
                *get_flag('suppa_per_local_event', suppa_per_local_event),
                *get_flag('suppa_per_isoform', suppa_per_isoform),
                *get_flag('suppa_tpm', suppa_tpm),
                *get_flag('generateevents_pool_genes', generateevents_pool_genes),
                *get_flag('generateevents_event_type', generateevents_event_type),
                *get_flag('generateevents_boundary', generateevents_boundary),
                *get_flag('generateevents_threshold', generateevents_threshold),
                *get_flag('generateevents_exon_length', generateevents_exon_length),
                *get_flag('psiperevent_total_filter', psiperevent_total_filter),
                *get_flag('diffsplice_local_event', diffsplice_local_event),
                *get_flag('diffsplice_isoform', diffsplice_isoform),
                *get_flag('diffsplice_method', diffsplice_method),
                *get_flag('diffsplice_area', diffsplice_area),
                *get_flag('diffsplice_lower_bound', diffsplice_lower_bound),
                *get_flag('diffsplice_gene_correction', diffsplice_gene_correction),
                *get_flag('diffsplice_paired', diffsplice_paired),
                *get_flag('diffsplice_alpha', diffsplice_alpha),
                *get_flag('diffsplice_median', diffsplice_median),
                *get_flag('diffsplice_tpm_threshold', diffsplice_tpm_threshold),
                *get_flag('diffsplice_nan_threshold', diffsplice_nan_threshold),
                *get_flag('clusterevents_local_event', clusterevents_local_event),
                *get_flag('clusterevents_isoform', clusterevents_isoform),
                *get_flag('clusterevents_sigthreshold', clusterevents_sigthreshold),
                *get_flag('clusterevents_dpsithreshold', clusterevents_dpsithreshold),
                *get_flag('clusterevents_eps', clusterevents_eps),
                *get_flag('clusterevents_metric', clusterevents_metric),
                *get_flag('clusterevents_separation', clusterevents_separation),
                *get_flag('clusterevents_min_pts', clusterevents_min_pts),
                *get_flag('clusterevents_method', clusterevents_method)
        ]

        print("Launching Nextflow Runtime")
        print(' '.join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("latch:///your_log_dir/nf_nf_core_rnasplice", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)



@workflow(metadata._nextflow_metadata)
def nf_nf_core_rnasplice(input: LatchFile, contrasts: str, outdir: typing_extensions.Annotated[LatchDir, FlyteAnnotation({'output': True})], email: typing.Optional[str], multiqc_title: typing.Optional[str], save_merged_fastq: typing.Optional[bool], genome: typing.Optional[str], fasta: typing.Optional[LatchFile], gtf: typing.Optional[LatchFile], gff: typing.Optional[LatchFile], transcript_fasta: typing.Optional[LatchFile], star_index: typing.Optional[str], salmon_index: typing.Optional[str], gencode: typing.Optional[bool], save_reference: typing.Optional[bool], multiqc_methods_description: typing.Optional[str], clip_r1: typing.Optional[int], clip_r2: typing.Optional[int], three_prime_clip_r1: typing.Optional[int], three_prime_clip_r2: typing.Optional[int], trim_nextseq: typing.Optional[int], save_trimmed: typing.Optional[bool], skip_trimming: typing.Optional[bool], skip_trimgalore_fastqc: typing.Optional[bool], skip_fastqc: typing.Optional[bool], bam_csi_index: typing.Optional[bool], star_ignore_sjdbgtf: typing.Optional[bool], salmon_quant_libtype: typing.Optional[str], seq_center: typing.Optional[str], skip_alignment: typing.Optional[bool], save_unaligned: typing.Optional[bool], save_align_intermeds: typing.Optional[bool], rmats: typing.Optional[bool], rmats_novel_splice_site: typing.Optional[bool], dexseq_exon: typing.Optional[bool], save_dexseq_annotation: typing.Optional[bool], gff_dexseq: typing.Optional[LatchFile], edger_exon: typing.Optional[bool], dexseq_dtu: typing.Optional[bool], sashimi_plot: typing.Optional[bool], suppa_tpm: typing.Optional[str], diffsplice_median: typing.Optional[bool], clusterevents_sigthreshold: typing.Optional[float], clusterevents_separation: typing.Optional[int], source: str = 'fastq', gtf_extra_attributes: typing.Optional[str] = 'gene_name', gtf_group_features: typing.Optional[str] = 'gene_id', min_trimmed_reads: typing.Optional[int] = 10000, skip_bigwig: typing.Optional[bool] = True, aligner: typing.Optional[str] = 'star', pseudo_aligner: typing.Optional[str] = 'salmon', rmats_splice_diff_cutoff: typing.Optional[float] = 0.0001, rmats_paired_stats: typing.Optional[bool] = True, rmats_read_len: typing.Optional[int] = 40, rmats_min_intron_len: typing.Optional[int] = 50, rmats_max_exon_len: typing.Optional[int] = 500, alignment_quality: typing.Optional[int] = 10, aggregation: typing.Optional[bool] = True, save_dexseq_plot: typing.Optional[bool] = True, n_dexseq_plot: typing.Optional[int] = 10, save_edger_plot: typing.Optional[bool] = True, n_edger_plot: typing.Optional[int] = 10, dtu_txi: typing.Optional[str] = 'dtuScaledTPM', min_samps_gene_expr: typing.Optional[int] = 6, min_samps_feature_expr: typing.Optional[int] = 0, min_samps_feature_prop: typing.Optional[int] = 0, min_gene_expr: typing.Optional[int] = 10, min_feature_expr: typing.Optional[int] = 10, min_feature_prop: typing.Optional[float] = 0.1, miso_genes: typing.Optional[str] = 'ENSG00000004961, ENSG00000005302', miso_genes_file: typing.Optional[str] = 'None', miso_read_len: typing.Optional[int] = 75, fig_height: typing.Optional[int] = 7, fig_width: typing.Optional[int] = 7, suppa: typing.Optional[bool] = True, suppa_per_local_event: typing.Optional[bool] = True, suppa_per_isoform: typing.Optional[bool] = True, generateevents_pool_genes: typing.Optional[bool] = True, generateevents_event_type: typing.Optional[str] = 'SE SS MX RI FL', generateevents_boundary: typing.Optional[str] = 'S', generateevents_threshold: typing.Optional[int] = 10, generateevents_exon_length: typing.Optional[int] = 100, psiperevent_total_filter: typing.Optional[int] = 0, diffsplice_local_event: typing.Optional[bool] = True, diffsplice_isoform: typing.Optional[bool] = True, diffsplice_method: typing.Optional[str] = 'empirical', diffsplice_area: typing.Optional[int] = 1000, diffsplice_lower_bound: typing.Optional[int] = 0, diffsplice_gene_correction: typing.Optional[bool] = True, diffsplice_paired: typing.Optional[bool] = True, diffsplice_alpha: typing.Optional[float] = 0.05, diffsplice_tpm_threshold: typing.Optional[int] = 0, diffsplice_nan_threshold: typing.Optional[int] = 0, clusterevents_local_event: typing.Optional[bool] = True, clusterevents_isoform: typing.Optional[bool] = True, clusterevents_dpsithreshold: typing.Optional[float] = 0.05, clusterevents_eps: typing.Optional[float] = 0.05, clusterevents_metric: typing.Optional[str] = 'euclidean', clusterevents_min_pts: typing.Optional[int] = 20, clusterevents_method: typing.Optional[str] = 'DBSCAN') -> None:
    """
    nf-core/rnasplice

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, input=input, contrasts=contrasts, source=source, outdir=outdir, email=email, multiqc_title=multiqc_title, save_merged_fastq=save_merged_fastq, genome=genome, fasta=fasta, gtf=gtf, gff=gff, transcript_fasta=transcript_fasta, star_index=star_index, salmon_index=salmon_index, gencode=gencode, gtf_extra_attributes=gtf_extra_attributes, gtf_group_features=gtf_group_features, save_reference=save_reference, multiqc_methods_description=multiqc_methods_description, clip_r1=clip_r1, clip_r2=clip_r2, three_prime_clip_r1=three_prime_clip_r1, three_prime_clip_r2=three_prime_clip_r2, trim_nextseq=trim_nextseq, save_trimmed=save_trimmed, skip_trimming=skip_trimming, skip_trimgalore_fastqc=skip_trimgalore_fastqc, min_trimmed_reads=min_trimmed_reads, skip_fastqc=skip_fastqc, skip_bigwig=skip_bigwig, aligner=aligner, pseudo_aligner=pseudo_aligner, bam_csi_index=bam_csi_index, star_ignore_sjdbgtf=star_ignore_sjdbgtf, salmon_quant_libtype=salmon_quant_libtype, seq_center=seq_center, skip_alignment=skip_alignment, save_unaligned=save_unaligned, save_align_intermeds=save_align_intermeds, rmats=rmats, rmats_splice_diff_cutoff=rmats_splice_diff_cutoff, rmats_paired_stats=rmats_paired_stats, rmats_read_len=rmats_read_len, rmats_novel_splice_site=rmats_novel_splice_site, rmats_min_intron_len=rmats_min_intron_len, rmats_max_exon_len=rmats_max_exon_len, dexseq_exon=dexseq_exon, save_dexseq_annotation=save_dexseq_annotation, gff_dexseq=gff_dexseq, alignment_quality=alignment_quality, aggregation=aggregation, save_dexseq_plot=save_dexseq_plot, n_dexseq_plot=n_dexseq_plot, edger_exon=edger_exon, save_edger_plot=save_edger_plot, n_edger_plot=n_edger_plot, dexseq_dtu=dexseq_dtu, dtu_txi=dtu_txi, min_samps_gene_expr=min_samps_gene_expr, min_samps_feature_expr=min_samps_feature_expr, min_samps_feature_prop=min_samps_feature_prop, min_gene_expr=min_gene_expr, min_feature_expr=min_feature_expr, min_feature_prop=min_feature_prop, sashimi_plot=sashimi_plot, miso_genes=miso_genes, miso_genes_file=miso_genes_file, miso_read_len=miso_read_len, fig_height=fig_height, fig_width=fig_width, suppa=suppa, suppa_per_local_event=suppa_per_local_event, suppa_per_isoform=suppa_per_isoform, suppa_tpm=suppa_tpm, generateevents_pool_genes=generateevents_pool_genes, generateevents_event_type=generateevents_event_type, generateevents_boundary=generateevents_boundary, generateevents_threshold=generateevents_threshold, generateevents_exon_length=generateevents_exon_length, psiperevent_total_filter=psiperevent_total_filter, diffsplice_local_event=diffsplice_local_event, diffsplice_isoform=diffsplice_isoform, diffsplice_method=diffsplice_method, diffsplice_area=diffsplice_area, diffsplice_lower_bound=diffsplice_lower_bound, diffsplice_gene_correction=diffsplice_gene_correction, diffsplice_paired=diffsplice_paired, diffsplice_alpha=diffsplice_alpha, diffsplice_median=diffsplice_median, diffsplice_tpm_threshold=diffsplice_tpm_threshold, diffsplice_nan_threshold=diffsplice_nan_threshold, clusterevents_local_event=clusterevents_local_event, clusterevents_isoform=clusterevents_isoform, clusterevents_sigthreshold=clusterevents_sigthreshold, clusterevents_dpsithreshold=clusterevents_dpsithreshold, clusterevents_eps=clusterevents_eps, clusterevents_metric=clusterevents_metric, clusterevents_separation=clusterevents_separation, clusterevents_min_pts=clusterevents_min_pts, clusterevents_method=clusterevents_method)

