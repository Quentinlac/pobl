-- ═══════════════════════════════════════════════════════════════════════════════
-- BTC 15-Minute Bot - Matrix Snapshots Table
-- ═══════════════════════════════════════════════════════════════════════════════
-- Stores probability matrix snapshots so both the cron job and bot can share data

CREATE TABLE IF NOT EXISTS matrix_snapshots (
    id              SERIAL PRIMARY KEY,

    -- Matrix data (stored as JSON)
    matrix_json     JSONB NOT NULL,

    -- Metadata
    total_windows   INTEGER NOT NULL,              -- Number of 15-min windows analyzed
    data_start      TIMESTAMPTZ,                   -- Earliest data point
    data_end        TIMESTAMPTZ,                   -- Latest data point

    -- Timestamps
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    -- Keep only recent snapshots (optional cleanup)
    is_active       BOOLEAN DEFAULT TRUE           -- Current active matrix
);

-- Index for quick lookup of active matrix
CREATE INDEX IF NOT EXISTS idx_matrix_snapshots_active ON matrix_snapshots(is_active, created_at DESC);

-- Function to get the latest active matrix
CREATE OR REPLACE FUNCTION get_latest_matrix()
RETURNS JSONB AS $$
BEGIN
    RETURN (
        SELECT matrix_json
        FROM matrix_snapshots
        WHERE is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql;

-- Function to save a new matrix (marks previous as inactive)
CREATE OR REPLACE FUNCTION save_matrix(
    p_matrix_json JSONB,
    p_total_windows INTEGER,
    p_data_start TIMESTAMPTZ,
    p_data_end TIMESTAMPTZ
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    -- Mark all previous as inactive
    UPDATE matrix_snapshots SET is_active = FALSE WHERE is_active = TRUE;

    -- Insert new matrix
    INSERT INTO matrix_snapshots (matrix_json, total_windows, data_start, data_end, is_active)
    VALUES (p_matrix_json, p_total_windows, p_data_start, p_data_end, TRUE)
    RETURNING id INTO v_id;

    -- Cleanup: keep only last 10 snapshots
    DELETE FROM matrix_snapshots
    WHERE id NOT IN (
        SELECT id FROM matrix_snapshots ORDER BY created_at DESC LIMIT 10
    );

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;
