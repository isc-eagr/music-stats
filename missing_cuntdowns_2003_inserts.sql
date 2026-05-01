-- Missing personal countdown inserts (provided lists)
-- Generated: 2026-04-28
-- Run this file alone to replace any existing entries for these dates.

BEGIN TRANSACTION;

-- Remove existing entries for these dates (idempotent)
DELETE FROM vatos_cuntdown_entry WHERE chart_date IN (
  '2003-05-26','2003-03-20','2003-03-03','2003-02-05','2003-02-25','2003-02-19','2003-02-21','2003-02-23',
  '2003-05-06','2003-05-15','2003-05-16','2003-05-20','2003-05-22','2003-06-07','2003-06-09','2003-06-10',
  '2003-06-16','2003-06-19'
);

-- 2003-05-26
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 10, 'Audioslave', 'Like a Stone', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 9, 'Kabah', 'Por Ti', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 8, 'Kylie Minogue', 'Your Disco Needs You', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 7, 'Jason Mraz', 'The Remedy', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 6, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 5, 'Lil Kim', 'The Jump Off', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 4, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 3, 'Within Temptation', 'Running Up That Hill', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 2, 'Sonata Arctica', 'Victoria''s Secret', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-26', 1, 'Sean Paul', 'Get Busy', 0, NULL);

-- 2003-03-20
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 10, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 9, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 8, 'JC Chasez', 'Blowin me up (With Her Love)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 7, 'TLC', 'Hands Up', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 6, 'Moby', 'In This World', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 5, 'Kelly Rowland', 'Can''t Nobody', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 4, 'Ms Dynamite', 'It Takes More', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 3, 'Mariah Carey', 'Boy (I Need You)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 2, 'Evanescence', 'Bring me to Life', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-20', 1, 'B2K', 'Girlfriend', 0, NULL);

-- 2003-03-03
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 10, 'Coldplay', 'Clocks', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 9, 'Ash', 'Burn Baby Burn', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 8, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 7, 'No Doubt', 'Running', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 6, 'TLC', 'Hands Up', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 5, 'Kelly Rowland', 'Can''t Nobody', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 4, 'Dj Sammy', 'Boys of Summer', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 3, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 2, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-03-03', 1, 'Moby', 'In This World', 0, NULL);

-- 2003-02-05
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 10, 'Kelly Rowland', 'Can''t Nobody', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 9, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 8, 'Timo Maas', 'Help Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 7, 'TLC', 'Hands Up', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 6, 'Snoop Dogg ft Pharrell', 'Beautiful', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 5, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 4, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 3, 'Moby', 'In This World', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 2, 'Dj Sammy', 'Boys of Summer', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-05', 1, 'Justin Timberlake', 'Rock Your Body', 0, NULL);

-- 2003-02-25
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 10, 'Kylie Minogue', 'Dancefloor', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 9, 'Timo Maas', 'Help Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 8, 'No Doubt', 'Running', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 7, 'Kelly Rowland', 'Can''t Nobody', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 6, 'Goldfinger', 'Superman', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 5, 'Moby', 'In This World', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 4, 'Ash', 'Burn Baby Burn', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 3, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 2, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-25', 1, 'TLC', 'Hands Up', 0, NULL);

-- 2003-02-19
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 10, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 9, 'Timo Maas', 'Help Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 8, 'Moby', 'In This World', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 7, 'Cirrus', 'Boomerang', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 6, 'Craig David', 'Hidden Agenda', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 5, 'Kylie Minogue', 'Dancefloor', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 4, 'Jay Z ft Beyonce', '''03 Bonnie & Clyde', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 3, 'Jennifer Lopez ft LL Cool J', 'All I Have', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 2, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-19', 1, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);

-- 2003-02-21
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 10, 'Cirrus', 'Boomerang', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 9, 'Ash', 'Burn Baby Burn', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 8, 'Jennifer Lopez ft LL Cool J', 'All I Have', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 7, 'Moby', 'In This World', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 6, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 5, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 4, 'Jay Z ft Beyonce', '''03 Bonnie & Clyde', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 3, 'Kylie Minogue', 'Dancefloor', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 2, 'Coldplay', 'Clocks', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-21', 1, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);

-- 2003-02-23 (Close Calls: positions 13..11 are treated as close calls)
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 0, 'Timo Maas', 'Help Me', 1, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 0, 'Kylie Minogue', 'Dancefloor', 1, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 0, 'Craig David', 'Hidden Agenda', 1, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 10, 'Cirrus', 'Boomerang', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 9, 'No Doubt', 'Running', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 8, 'Jay Z ft Beyonce', '''03 Bonnie & Clyde', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 7, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 6, 'Goldfinger', 'Superman', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 5, 'Coldplay', 'Clocks', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 4, 'Moby', 'In This World', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 3, 'Shakira', 'The One', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 2, 'Ja Rule ft Ashanti', 'Mesmerize', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-02-23', 1, 'Ash', 'Burn Baby Burn', 0, NULL);

-- 2003-05-06
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 10, 'Who da Funk', 'Shiny Disco Balls', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 9, 'Jennifer Lopez', 'I''m Glad', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 8, 'TLC', 'Damaged', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 7, 'Audioslave', 'Like a Stone', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 6, 'Mariah Carey', 'Boy (I Need You)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 5, 'Boomkat', 'The Wreckoning', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 4, 'Eve 6', 'Here''s to the Night', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 3, 'Placebo', 'The Bitter End', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 2, 'Sonata Arctica', 'Victoria''s Secret', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-06', 1, 'Moby', 'Sunday (The Day Before My Birthday)', 0, NULL);

-- 2003-05-15
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 10, 'Jennifer Lopez', 'I''m Glad', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 9, 'Eminem', 'Sing For The Moment', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 8, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 7, 'Kylie Minogue', 'Confide in Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 6, 'Kabah', 'Por Ti', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 5, 'TLC', 'Damaged', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 4, 'Kylie Minogue', 'Please Stay', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 3, 'Placebo', 'The Bitter End', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 2, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-15', 1, 'Audioslave', 'Like a Stone', 0, NULL);

-- 2003-05-16
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 10, 'Kabah', 'Por Ti', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 9, 'TLC', 'Damaged', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 8, 'Jennifer Lopez', 'I''m Glad', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 7, 'Kylie Minogue', 'Please Stay', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 6, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 5, 'Kylie Minogue', 'Confide in Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 4, 'Audioslave', 'Like a Stone', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 3, 'Placebo', 'The Bitter End', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 2, 'Lil Kim', 'The Jump Off', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-16', 1, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);

-- 2003-05-20
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 10, 'Eminem', 'Sing For The Moment', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 9, 'Lasgo', 'Something', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 8, '50 Cent', '21 Questions', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 7, 'Sugababes', 'Shape', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 6, 'Within Temptation', 'Running Up That Hill', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 5, 'Christina Aguilera', 'Fighter', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 4, 'Kabah', 'Por Ti', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 3, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 2, 'Mandy Moore', 'Crush', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-20', 1, 'Justin Timberlake', 'Take it From Here', 0, NULL);

-- 2003-05-22
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 10, '50 Cent', '21 Questions', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 9, 'Audioslave', 'Like a Stone', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 8, 'Within Temptation', 'Running Up That Hill', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 7, 'Kylie Minogue', 'Confide in Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 6, 'Mandy Moore', 'Crush', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 5, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 4, 'Christina Aguilera', 'Fighter', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 3, 'Lillix', 'It''s About Time', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 2, 'Lil Kim', 'The Jump Off', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-05-22', 1, 'Sean Paul', 'Get Busy', 0, NULL);

-- 2003-06-07
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 10, 'Christina Aguilera', 'Fighter', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 9, 'Roxette', 'It Must Have Been Love', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 8, 'Scooter', 'Weekend', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 7, 'Craig David', 'Fill Me In', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 6, 'Sugababes', 'Shape', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 5, 'Ashanti', 'Rock Wit U (Awww Baby)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 4, 'Matchbox 20', 'Unwell', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 3, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 2, 'Mandy Moore', 'Cry', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-07', 1, 'HIM', 'Join Me', 0, NULL);

-- 2003-06-09
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 10, 'Sugababes', 'Shape', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 9, 'Christina Aguilera', 'Fighter', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 8, 'Roxette', 'It Must Have Been Love', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 7, 'Kelly Clarkson', 'Miss Independent', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 6, 'Ashanti', 'Rock Wit U (Awww Baby)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 5, 'Kylie Minogue', 'Did it Again', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 4, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 3, 'HIM', 'Join Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 2, 'Matchbox 20', 'Unwell', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-09', 1, 'Mandy Moore', 'Cry', 0, NULL);

-- 2003-06-10
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 10, 'Kylie Minogue', 'Did it Again', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 9, 'Ashanti', 'Rock Wit U (Awww Baby)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 8, 'Kelly Clarkson', 'Miss Independent', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 7, 'Within Temptation', 'Running Up That Hill', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 6, 'Kylie Minogue', 'Breathe', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 5, 'Christina Aguilera', 'Fighter', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 4, 'Lisa Loeb', 'Someone You Should Know', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 3, 'Matchbox 20', 'Unwell', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 2, 'Mandy Moore', 'Cry', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-10', 1, 'HIM', 'Join Me', 0, NULL);

-- 2003-06-16
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 10, 'HIM', 'Join Me', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 9, 'Lil Kim ft 50 Cent', 'Magic Stick', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 8, 'Mya', 'My Love Is Like Wo', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 7, 'Dune', 'Million Miles From Home', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 6, 'Royksopp', 'Sparks', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 5, 'Sixpence None The Richer', 'Breathe Your Name', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 4, 'Mandy Moore', 'Cry', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 3, 'Matchbox 20', 'Unwell', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 2, 'Paul Oakenfold', 'Southern Sun', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-16', 1, 'Divinyls', 'I Touch Myself', 0, NULL);

-- 2003-06-19
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 10, 'Audioslave', 'Like a Stone', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 9, 'Avril Lavigne', 'Losing Grip', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 8, 'Placebo', 'The Bitter End', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 7, 'Monica', 'So Gone', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 6, 'Royksopp', 'Sparks', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 5, 'Jewel', 'Intuition', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 4, 'The Cardigans', 'For What It''s Worth', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 3, 'Sixpence None the Richer', 'Breathe Your Name', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 2, 'Ashanti', 'Rock Wit U (Awww Baby)', 0, NULL);
INSERT INTO vatos_cuntdown_entry (chart_date, position, artist_name, song_title, is_close_call, song_id) VALUES ('2003-06-19', 1, 'Mandy Moore', 'Cry', 0, NULL);

COMMIT;

-- End of missing countdown inserts
