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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    description="Syncs the previous day's Uhai traveler screenings every day at 04:00 UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
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
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

nphiis_sync_job = dg.define_asset_job(
    name="nphiis_sync_job",
    selection=dg.AssetSelection.groups("nphiis"),
    description="Loads NPHIIS VHF case and contact line lists for one partition (day) into MinIO",
)

# daily-partitioned job -> schedule fires at 02:00 UTC for the previous day
sync_nphiis_daily = dg.build_schedule_from_partitioned_job(
    nphiis_sync_job,
    hour_of_day=2,
    name="sync_nphiis_daily",
    description="Syncs the previous day's NPHIIS VHF line lists every day at 02:00 UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
