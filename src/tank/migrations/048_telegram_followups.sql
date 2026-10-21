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
