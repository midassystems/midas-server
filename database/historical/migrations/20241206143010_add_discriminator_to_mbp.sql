-- Add migration script here
ALTER TABLE mbp
ADD COLUMN discriminator INTEGER NOT NULL;

-- Update the unique constraint to include the discriminator field
ALTER TABLE mbp
DROP CONSTRAINT unique_instrument_ts_sequence_event;

ALTER TABLE mbp
ADD CONSTRAINT unique_instrument_ts_sequence_event
  UNIQUE (instrument_id, ts_event, price, size, flags, sequence, order_book_hash, ts_recv, action, side, discriminator);

