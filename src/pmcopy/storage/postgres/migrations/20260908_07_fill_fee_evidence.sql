-- Older accumulators defaulted absent fees to zero, so historical fee
-- provenance remains unknown until authoritative fee evidence is imported.
ALTER TABLE fills ADD COLUMN IF NOT EXISTS fee_evidence_known boolean NOT NULL DEFAULT false;
