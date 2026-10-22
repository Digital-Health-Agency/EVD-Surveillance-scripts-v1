import datetime as dt

import dagster as dg

mdharura_sync_job = dg.define_asset_job(
    name="mdharura_sync_job",
    selection=dg.AssetSelection.groups("mdharura"),
    description="Loads m-Dharura signals for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 06:00 UTC for the previous day
sync_mdharura_signals_daily = dg.build_schedule_from_partitioned_job(
    mdharura_sync_job,
    hour_of_day=6,
    name="sync_mdharura_signals_daily",
    description="Syncs the previous day's signals from m-Dharura every day at 06:00 UTC",
)

adam_sync_job = dg.define_asset_job(
    name="adam_sync_job",
    selection=dg.AssetSelection.groups("adam"),
    description="Loads ADaM cases and travellers for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 05:00 UTC for the previous day
sync_adam_daily = dg.build_schedule_from_partitioned_job(
    adam_sync_job,
    hour_of_day=5,
    name="sync_adam_daily",
    description="Syncs the previous day's cases and travellers from ADaM every day at 05:00 UTC",
)

lims_sync_job = dg.define_asset_job(
    name="lims_sync_job",
    selection=dg.AssetSelection.groups("lims"),
    description="Loads LIMS results for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 03:00 UTC for the previous day
sync_lims_results_daily = dg.ScheduleDefinition(
    job=lims_sync_job,
    cron_schedule="0 3 * * *",
    name="sync_lims_results_daily",
    description="Syncs results from LIMS every day at 03:00 UTC",
)

uhai_sync_job = dg.define_asset_job(
    name="uhai_sync_job",
    selection=dg.AssetSelection.groups("uhai"),
    description="Loads Uhai traveler screenings for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 04:00 UTC for the previous day
sync_uhai_screenings_daily = dg.build_schedule_from_partitioned_job(
    uhai_sync_job,
    hour_of_day=4,
    name="sync_uhai_screenings_daily",
    description="Syncs the previous day's Uhai traveler screenings every day at 04:00 UTC"
)
cbs_sync_job = dg.define_asset_job(
    name="cbs_sync_job",
    selection=dg.AssetSelection.groups("cbs"),
    description="Loads CBS reports and screenings for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 06:00 UTC for the previous day
sync_cbs_daily = dg.build_schedule_from_partitioned_job(
    cbs_sync_job,
    hour_of_day=6,
    name="sync_cbs_daily",
    description="Syncs the previous day's CBS reports and screenings every day at 06:00 UTC",
)
krcs_evd_screening_sync_job = dg.define_asset_job(
    name="krcs_evd_screening_sync_job",
    selection=dg.AssetSelection.groups("krcs_evd_screening"),
    description="Loads PoE health screenings for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 07:00 UTC for the previous day
sync_krcs_evd_screening_screenings_daily = dg.build_schedule_from_partitioned_job(
    krcs_evd_screening_sync_job,
    hour_of_day=7,
    name="sync_krcs_evd_screening_screenings_daily",
    description="Syncs the previous day's PoE health screenings every day at 07:00 UTC",
)

echis_sync_job = dg.define_asset_job(
    name="echis_sync_job",
    selection=dg.AssetSelection.groups("echis"),
    description="Loads echis results for one partition (day) into MinIO",
)

sync_echis_results_daily = dg.ScheduleDefinition(
    job=echis_sync_job,
    cron_schedule="0 1 * * *",
    name="sync_echis_results_daily",
    description="Syncs results from echis every day at 01:00 UTC",
)


krcs_evd_quarantine_sync_job = dg.define_asset_job(
    name="krcs_evd_quarantine_sync_job",
    selection=dg.AssetSelection.groups("krcs_evd_quarantine"),
    description="Loads EVD quarantine records for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 08:00 UTC for the previous day
sync_krcs_evd_quarantine_daily = dg.build_schedule_from_partitioned_job(
    krcs_evd_quarantine_sync_job,
    hour_of_day=8,
    name="sync_krcs_evd_quarantine_daily",
    description="Syncs the previous day's EVD quarantine records every day at 08:00 UTC",
)

taifa_care_kenyaemr_sync_job = dg.define_asset_job(
    name="taifa_care_kenyaemr_sync_job",
    selection=dg.AssetSelection.groups("taifa_care_kenyaemr"),
    description="Loads Taifa Care flagged cases for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 08:00 UTC for the previous day
sync_taifa_care_kenyaemr_daily = dg.build_schedule_from_partitioned_job(
    taifa_care_kenyaemr_sync_job,
    hour_of_day=8,
    name="sync_taifa_care_kenyaemr_daily",
    description="Syncs the previous day's Taifa Care flagged cases every day at 08:00 UTC",
)

# one asset per configured HMIS instance; the partitions are given here (matching
# tiberbu_hmis/defs.yaml) so the job still builds while no instance is configured
tiberbu_hmis_sync_job = dg.define_asset_job(
    name="tiberbu_hmis_sync_job",
    selection=dg.AssetSelection.groups("tiberbu_hmis"),
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-06-01"),
    description="Loads Ebola screenings from every Tiberbu HMIS instance for one partition (day) into MinIO",
)

# Runs carrying this tag key queue one at a time (tag_concurrency_limits in
# deploy/dagster.yaml): the instances share one dlt pipeline.
TIBERBU_HMIS_RUN_TAG = "tiberbu_hmis/instance"


@dg.schedule(
    job=tiberbu_hmis_sync_job,
    cron_schedule="0 9 * * *",
    execution_timezone="UTC",
    name="sync_tiberbu_hmis_screenings_daily",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description=(
        "Syncs the previous day's Ebola screenings from each Tiberbu HMIS instance every day "
        "at 09:00 UTC, one run per instance, so an instance that is down fails only its own run"
    ),
)
def sync_tiberbu_hmis_screenings_daily(context: dg.ScheduleEvaluationContext):
    partition = (context.scheduled_execution_time - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    asset_keys = dg.AssetSelection.groups("tiberbu_hmis").resolve(context.repository_def.asset_graph)
    for key in sorted(asset_keys, key=lambda k: k.to_user_string()):
        instance = key.path[-1].removeprefix("screenings_")
        yield dg.RunRequest(
            run_key=f"{partition}:{key.to_user_string()}",
            partition_key=partition,
            asset_selection=[key],
            tags={TIBERBU_HMIS_RUN_TAG: instance},
        )
