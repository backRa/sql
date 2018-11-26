ALTER PROCEDURE [db_logic].[sp_writeVariable]
    @var_name        VARCHAR(50),
    @insert_job      BIGINT = NULL,
    @fetch_job       BIGINT = NULL,
    @insert_job_time DATETIME = NULL,
    @count_check     BIGINT = NULL,
    @fetch_job_time  DATETIME = NULL,
    @flag            SMALLINT = NULL,
    @comment         VARCHAR(1000) = NULL
AS
  BEGIN

    MERGE db_logic.variable WITH ( HOLDLOCK ) TARGET
    USING (SELECT
             @var_name        AS var_name,
             @insert_job      AS insert_job,
             @fetch_job       AS fetch_job,
             @insert_job_time AS insert_job_time,
             @count_check     AS count_check,
             @fetch_job_time  AS fetch_job_time,
             @flag            AS flag,
             @comment         AS comment
          ) AS SOURCE
    ON (TARGET.var_name = SOURCE.var_name)
    WHEN MATCHED THEN
    UPDATE SET TARGET.var_name = SOURCE.var_name
      , TARGET.insert_job      = isnull(SOURCE.insert_job, TARGET.insert_job)
      , TARGET.fetch_job       = isnull(SOURCE.fetch_job, TARGET.fetch_job)
      , TARGET.insert_job_time = isnull(SOURCE.insert_job_time, TARGET.insert_job_time)
      , TARGET.count_check     = isnull(SOURCE.count_check, TARGET.count_check)
      , TARGET.fetch_job_time  = isnull(SOURCE.fetch_job_time, TARGET.fetch_job_time)
      , TARGET.flag            = isnull(SOURCE.flag, TARGET.flag)
      , TARGET.comment         = isnull(SOURCE.comment, TARGET.comment)
    WHEN NOT MATCHED BY TARGET THEN
    INSERT (var_name, insert_job, fetch_job, insert_job_time, count_check, fetch_job_time, flag, comment)
    VALUES (SOURCE.var_name, SOURCE.insert_job, SOURCE.fetch_job, SOURCE.insert_job_time, SOURCE.count_check,
            SOURCE.fetch_job_time, SOURCE.flag, comment);
  END;
