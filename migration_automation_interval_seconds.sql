-- Automated Last.fm import interval in seconds (19/11/2026).
-- The app adds this column itself on startup (PlayAutomationStateService), so only run this
-- by hand on a database the new build has not opened yet; on one it already opened it fails with "duplicate column".
-- The interval setting moves from app_config 'automation.intervalMinutes' to 'automation.intervalSeconds'
-- automatically on startup as well (old value x 60).
ALTER TABLE play_import_automation_log ADD COLUMN interval_seconds INTEGER;
