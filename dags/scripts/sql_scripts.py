JOIN_TRACKID_RIGHTHOLDERID = """
    SELECT
      listen_daily.track_id,
      COUNT(listen_daily.track_id) AS track_rightsholder_count,
      rightsholder_info.rightsholder_id,
    FROM (
      SELECT
        *
      FROM
        `scchallenge.listen_*` as _listen_daily
      WHERE _listen_daily.event_time>='{{ retrieve_start_timestamp(ds) }}'
      AND _listen_daily.event_time<'{{ retrieve_end_timestamp(ds) }}'
      AND _listen_daily.duration >= 30
    )
    as listen_daily
    JOIN scchallenge.track_rightsholder_rollup as rightsholder_info
    ON listen_daily.track_id = rightsholder_info.track_id
      AND listen_daily.event_time >= rightsholder_info.valid_from
      AND listen_daily.event_time < rightsholder_info.valid_to
    GROUP BY listen_daily.track_id, rightsholder_info.rightsholder_id
"""

JOIN_TRACKID_TRACKTITLE = """
    SELECT
      weekly_listens.track_id,
      track_rightsholder_count,
      rightsholder_id,
      track_title
    FROM
      `scchallenge.weekly_track_rightsholder_{{ retrieve_start_date(ds) }}` AS weekly_listens
    JOIN
      (
        SELECT
          track_id,
          track_title,
          row_number() over (partition by track_id order by track_title) AS rn
         FROM
           `scchallenge.track_information_rollup`
      ) AS track_information
    ON weekly_listens.track_id = track_information.track_id
    WHERE rn = 1
"""

JOIN_RIGHTSHOLDER_PAYOUT = """
    SELECT
      weekly_rightsholder.rightsholder_id,
      SUM(track_rightsholder_count) AS total_plays,
      SUM(amount) AS weekly_payout,
      SUM(amount) / SUM(track_rightsholder_count) AS per_track_payout
    FROM scchallenge.weekly_track_title_rightsholder_{{ retrieve_start_date(ds) }} AS weekly_rightsholder
    JOIN scchallenge.payout_{{ retrieve_start_date(ds) }} as payout_rightsholder
    ON weekly_rightsholder.rightsholder_id = payout_rightsholder.rightsholder_id
    GROUP BY rightsholder_id
"""

JOIN_TRACKID_RIGHTHOLDER_PAYOUT = """
    SELECT
      '{{ retrieve_start_date(ds) }}' AS reporting_period_start_date,
      '{{ retrieve_end_date(ds) }}' AS reporting_period_end_date,
      track_id,
      track_title,
      weekly_listens.rightsholder_id,
      track_rightsholder_count as total_plays,
      per_track_payout as unit_price
    FROM
      `scchallenge.weekly_track_title_rightsholder_{{ retrieve_start_date(ds) }}` AS weekly_listens
    JOIN
      `scchallenge.weekly_rightsholder_payout_{{ retrieve_start_date(ds) }}` AS weekly_payout
    ON weekly_listens.rightsholder_id = weekly_payout.rightsholder_id
"""
