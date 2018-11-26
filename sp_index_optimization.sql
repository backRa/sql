ALTER PROCEDURE
  [db_logic].[sp_index_optimization]
  (
    @defragmentation_min FLOAT = 5,
    @defragmentation_max FLOAT = 30,
    @table_name          VARCHAR(256),
    @db_name             VARCHAR(256) = 'ors',
    @daily_limit_ignore  BIT = 0
  ) AS
  BEGIN

    IF OBJECT_ID('tempdb..#temp') IS NOT NULL
      DROP TABLE #temp;

    DECLARE @errorMessage VARCHAR(2048)
    SELECT
      a.index_id,
      name AS index_name,
      avg_fragmentation_in_percent
    INTO #temp
    FROM sys.dm_db_index_physical_stats(DB_ID(@db_name),
                                        OBJECT_ID(@table_name), NULL, NULL, NULL) AS a
      JOIN sys.indexes AS b
        ON a.object_id = b.object_id AND a.index_id = b.index_id
    ORDER BY avg_fragmentation_in_percent DESC

    DELETE FROM #temp
    WHERE avg_fragmentation_in_percent < @defragmentation_min

    WHILE (SELECT count(*)
           FROM #temp
           WHERE index_name IS NOT NULL) > 0
      BEGIN
        DECLARE @current_name VARCHAR(256),
        @current_index_id INT,
        @current_fragmentation FLOAT,
        @action VARCHAR(32),
        @sql VARCHAR(1024)

        SELECT TOP 1
          @current_name = index_name,
          @current_index_id = index_id,
          @current_fragmentation = avg_fragmentation_in_percent
        FROM #temp

        IF @current_fragmentation > @defragmentation_max
          BEGIN
            SET @action = 'REBUILD'
            SET @sql = 'ALTER INDEX ' + @current_name + ' ON ' + @table_name +
                       ' REBUILD WITH (ONLINE = ON, SORT_IN_TEMPDB = ON)'
            BEGIN TRY
            EXEC (@sql)
            END TRY
            BEGIN CATCH

            SET @errorMessage = CONCAT('ErrorLine', ':', ERROR_LINE(), ', ErrorMessage', ':', ERROR_MESSAGE())
            END CATCH
          END
        ELSE
          BEGIN
            SET @action = 'REORGANIZE'
            SET @sql = 'ALTER INDEX ' + @current_name + ' ON ' + @table_name + ' REORGANIZE';

            BEGIN TRY
            EXEC (@sql)
            END TRY
            BEGIN CATCH

            SET @errorMessage = CONCAT('ErrorLine', ':', ERROR_LINE(), ', ErrorMessage', ':', ERROR_MESSAGE())
            END CATCH
          END

        INSERT INTO db_logic.index_optimization_log (table_name, index_name,
                                                     avg_fragmentation_prc,
                                                     action, errorMessage, errorFlg)
        VALUES
          (@table_name, @current_name, @current_fragmentation, @action, @errorMessage, iif(@errorMessage IS NULL, 0, 1))

        SET @errorMessage = NULL

        DELETE FROM #temp
        WHERE index_id = @current_index_id

        PRINT concat('Applied action ', @action, ' on ', @table_name, '.', @current_name, ': ', @current_fragmentation)

      END
  END
