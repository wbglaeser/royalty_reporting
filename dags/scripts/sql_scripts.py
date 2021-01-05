SELECT_RELEVANT_PLAYS = """
    SELECT
      *
    FROM
      `{{ input_dataset_id }}.listen_*` AS _listen_daily
    WHERE (
      _TABLE_SUFFIX = '{{ retrieve_date(ds, "monday")}}'
      OR _TABLE_SUFFIX = '{{ retrieve_date(ds, "tuesday")}}'
      OR _TABLE_SUFFIX = '{{ retrieve_date(ds, "wednesday")}}'
      OR _TABLE_SUFFIX = '{{ retrieve_date(ds, "thursday")}}'
      OR _TABLE_SUFFIX = '{{ retrieve_date(ds, "friday")}}'
      OR _TABLE_SUFFIX = '{{ retrieve_date(ds, "saturday")}}'
      OR _TABLE_SUFFIX = '{{ retrieve_date(ds, "sunday")}}'
    )
      AND _listen_daily.event_time>='{{ retrieve_start_timestamp(ds) }}'
      AND _listen_daily.event_time<'{{ retrieve_end_timestamp(ds) }}'
      AND _listen_daily.duration >= 30
"""

JOIN_TRACKID_RIGHTHOLDERID = """
    SELECT
      listen_daily.track_id,
      COUNT(listen_daily.track_id) AS track_count_per_rightsholder,
      rightsholder_info.rightsholder_id,
    FROM `{{ relevant_plays }}_{{ retrieve_start_date(ds) }}` AS listen_daily
    JOIN {{ input_dataset_id }}.track_rightsholder_rollup as rightsholder_info
    ON listen_daily.track_id = rightsholder_info.track_id
      AND listen_daily.event_time >= CAST(rightsholder_info.valid_from AS TIMESTAMP)
      AND listen_daily.event_time < CAST(rightsholder_info.valid_to +1 AS TIMESTAMP)
    GROUP BY listen_daily.track_id, rightsholder_info.rightsholder_id
"""

JOIN_TRACKID_TRACKTITLE = """
    SELECT
      weekly_listens.track_id,
      track_count_per_rightsholder,
      rightsholder_id,
      track_title
    FROM
      `{{ plays_rightsholder }}_{{ retrieve_start_date(ds) }}` AS weekly_listens
    JOIN
      (
        SELECT
          track_id,
          track_title,
          row_number() over (partition by track_id order by track_title) AS rn
         FROM
           `{{ input_dataset_id }}.track_information_rollup`
      ) AS track_information
    ON weekly_listens.track_id = track_information.track_id
    WHERE rn = 1
"""

JOIN_RIGHTSHOLDER_PAYOUT = """
    SELECT
      weekly_rightsholder.rightsholder_id,
      SUM(track_count_per_rightsholder) AS total_plays,
      amount AS weekly_payout,
      amount / SUM(track_count_per_rightsholder) AS per_track_payout
    FROM {{ plays_titles_rightsholder }}_{{ retrieve_start_date(ds) }} AS weekly_rightsholder
    JOIN {{ input_dataset_id }}.payout_{{ retrieve_start_date(ds) }} as payout_rightsholder
    ON weekly_rightsholder.rightsholder_id = payout_rightsholder.rightsholder_id
    GROUP BY amount, rightsholder_id
"""

JOIN_TRACKID_RIGHTHOLDER_PAYOUT = """
    SELECT
      '{{ retrieve_start_date(ds) }}' AS reporting_period_start_date,
      '{{ retrieve_end_date(ds) }}' AS reporting_period_end_date,
      track_id,
      track_title,
      weekly_listens.rightsholder_id,
      track_count_per_rightsholder as total_plays,
      per_track_payout as unit_price
    FROM
      `{{ plays_titles_rightsholder }}_{{ retrieve_start_date(ds) }}` AS weekly_listens
    JOIN
      `{{ weekly_rightsholder_payout }}_{{ retrieve_start_date(ds) }}` AS weekly_payout
    ON weekly_listens.rightsholder_id = weekly_payout.rightsholder_id
"""
