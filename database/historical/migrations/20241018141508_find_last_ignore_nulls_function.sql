-- Add migration script here
-- BBO Required Function
CREATE FUNCTION coalesce_r_sfunc(state anyelement, value anyelement)
RETURNS anyelement
IMMUTABLE PARALLEL SAFE
AS $$
    SELECT COALESCE(value, state);
$$ LANGUAGE sql;

CREATE AGGREGATE find_last_ignore_nulls(anyelement) (
    SFUNC = coalesce_r_sfunc,
    STYPE = anyelement
);
