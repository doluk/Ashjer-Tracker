CREATE SCHEMA IF NOT EXISTS public;
CREATE EXTENSION pg_trgm;
CREATE EXTENSION fuzzystrmatch;
CREATE TABLE IF NOT EXISTS public.accounts (
    account_tag TEXT PRIMARY KEY,
    account_name TEXT,
    tracking_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP with time zone,
    ts tsvector GENERATED ALWAYS AS (setweight(to_tsvector('simple', account_name), 'A')) STORED
);

CREATE TABLE IF NOT EXISTS public.account_tracking (
    account_tag TEXT REFERENCES public.accounts (account_tag) on delete cascade on update cascade,
    first_observed TIMESTAMP with time zone not null,
    times_observed int default 1,
    requested_at TIMESTAMP with time zone not null,
    builder_hall_level int, -- builderHallLevel
    builder_base_trophies int, -- builderBaseTrophies
    best_builder_base_trophies int, -- bestBuilderBaseTrophies
    builder_base_league int, -- builderBaseLeague.id
    best_builder_base_season_id text,  -- legendStatistics.bestBuilderBaseSeason.id
    best_builder_base_season_rank int,  -- legendStatistics.bestBuilderBaseSeason.rank
    best_builder_base_season_trophies int,  -- legendStatistics.bestBuilderBaseSeason.trophies
    previous_builder_base_season_id text,  -- legendStatistics.previousBuilderBaseSeason.id
    previous_builder_base_season_rank int,  -- legendStatistics.previousBuilderBaseSeason.rank
    previous_builder_base_season_trophies int,  -- legendStatistics.previousBuilderBaseSeason.trophies
    builder_base_halls_destroyed int, -- Un-Build It
    builder_base_trophies_achievement int, -- Champion Builder
    PRIMARY KEY (account_tag, requested_at)
);

CREATE INDEX IF NOT EXISTS player_tracking_playertag_idx ON public.account_tracking (account_tag);
CREATE INDEX IF NOT EXISTS player_tracking_requested_at_idx ON public.account_tracking (requested_at);

CREATE TABLE IF NOT EXISTS public.players (
    player_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_discord_id bigint unique,
    player_name text not null,
    ts tsvector GENERATED ALWAYS AS (setweight(to_tsvector('simple', player_name), 'A')) STORED
);

CREATE TABLE IF NOT EXISTS public.player_accounts (
    player_id int references players(player_id) on delete cascade on update cascade not null,
    account_tag text references accounts(account_tag) on delete cascade on update cascade unique not null,
    PRIMARY KEY (player_id, account_tag)
);
DROP TABLE public.account_tracking_v2;
CREATE TABLE IF NOT EXISTS public.account_tracking_v2 (
    account_tag text not null, -- tag
    account_name text not null,
    requested_at TIMESTAMP with time zone not null,
    builder_base_trophies int not null, -- builderBaseTrophies
    PRIMARY KEY (account_tag, requested_at)
);
CREATE INDEX IF NOT EXISTS leaderboards_requested_at_idx ON public.leaderboards (requested_at);
CREATE INDEX IF NOT EXISTS leaderboards_tag_idx ON public.leaderboards (account_tag);
CREATE INDEX IF NOT EXISTS leaderboards_ranking_idx ON public.leaderboards (requested_at, current_rank);

