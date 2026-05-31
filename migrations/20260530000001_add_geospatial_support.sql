-- Enable PostGIS extension for geospatial queries
CREATE EXTENSION IF NOT EXISTS earthdistance CASCADE;

-- Add geospatial columns to events table
ALTER TABLE events 
ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- Create spatial index for geospatial queries
CREATE INDEX IF NOT EXISTS idx_events_location 
ON events USING btree (latitude, longitude) 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Create function to extract lat/lon from event_data JSONB
CREATE OR REPLACE FUNCTION extract_coordinates_from_event_data()
RETURNS TRIGGER AS $$
BEGIN
    -- Extract latitude and longitude from event_data if they exist
    IF NEW.event_data ? 'latitude' AND NEW.event_data ? 'longitude' THEN
        NEW.latitude := (NEW.event_data->>'latitude')::DOUBLE PRECISION;
        NEW.longitude := (NEW.event_data->>'longitude')::DOUBLE PRECISION;
    ELSIF NEW.event_data ? 'location' AND NEW.event_data->'location' ? 'lat' AND NEW.event_data->'location' ? 'lon' THEN
        NEW.latitude := (NEW.event_data->'location'->>'lat')::DOUBLE PRECISION;
        NEW.longitude := (NEW.event_data->'location'->>'lon')::DOUBLE PRECISION;
    ELSIF NEW.event_data ? 'coordinates' AND jsonb_array_length(NEW.event_data->'coordinates') >= 2 THEN
        NEW.longitude := (NEW.event_data->'coordinates'->0)::DOUBLE PRECISION;
        NEW.latitude := (NEW.event_data->'coordinates'->1)::DOUBLE PRECISION;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically extract coordinates on insert/update
DROP TRIGGER IF EXISTS trigger_extract_coordinates ON events;
CREATE TRIGGER trigger_extract_coordinates
    BEFORE INSERT OR UPDATE ON events
    FOR EACH ROW
    EXECUTE FUNCTION extract_coordinates_from_event_data();