-- Telegram follow-ups: raw bot updates and the reader questions that continue an agent session.
CREATE TABLE IF NOT EXISTS ops.telegram_messages (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"chat_id" text,
"message_id" bigint,
"user_id" text,
"status" text);
CREATE INDEX IF NOT EXISTS ops_telegram_messages_chat_user_idx ON ops.telegram_messages ("chat_id", "user_id");
CREATE INDEX IF NOT EXISTS ops_telegram_messages_status_idx ON ops.telegram_messages ("status");
CREATE INDEX IF NOT EXISTS ops_telegram_messages_doc_idx ON ops.telegram_messages USING gin(document);
CREATE TABLE IF NOT EXISTS out.user_questions (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"question_id" text,
"session_id" text,
"event_id" text,
"status" text);
CREATE INDEX IF NOT EXISTS out_user_questions_session_id_idx ON out.user_questions ("session_id");
CREATE INDEX IF NOT EXISTS out_user_questions_status_idx ON out.user_questions ("status");
CREATE INDEX IF NOT EXISTS out_user_questions_doc_idx ON out.user_questions USING gin(document);
-- Answers and prompts to readers are a third delivery subject next to alerts and ops notices.
ALTER TABLE out.deliveries ADD COLUMN IF NOT EXISTS "question_id" text;
ALTER TABLE out.deliveries DROP CONSTRAINT IF EXISTS delivery_subject;
ALTER TABLE out.deliveries ADD CONSTRAINT delivery_subject CHECK (
    (alert_id IS NOT NULL AND version IS NOT NULL AND notice_id IS NULL AND question_id IS NULL)
    OR (alert_id IS NULL AND version IS NULL AND notice_id IS NOT NULL AND question_id IS NULL)
    OR (alert_id IS NULL AND version IS NULL AND notice_id IS NULL AND question_id IS NOT NULL));
CREATE INDEX IF NOT EXISTS out_deliveries_question_id_idx ON out.deliveries ("question_id");
CREATE INDEX IF NOT EXISTS out_deliveries_message_ref_idx ON out.deliveries ("channel", "message_ref");
-- Files a reader attaches to a follow-up; the agent reads them read-only into its workspace.
CREATE TABLE IF NOT EXISTS ops.telegram_files (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"question_id" text,
"session_id" text,
"filename" text,
"size" bigint);
CREATE INDEX IF NOT EXISTS ops_telegram_files_question_id_idx ON ops.telegram_files ("question_id");
