# Состояние Tank — 20 октября 2026

**Задача ACTIVE. Полная приёмка не завершена.**

Основной `tank serve` работает без X на r81: Tank `94a6ac79…`, код `91b9a765…`, OpenMA `da8d1b4c…`, analyst context-9/data-catalog-2. Сверены все 349 файлов образа. Конфигурация и ресурсы primary/QA разделены. [Независимая проверка primary](verification/requirements-integration-2026-10-20/primary-r81-independent-audit.json).

## Подтверждено

- **Последние 27 требований:** изменения внесены; есть компонентные проверки публикации, карточек/решений, источников, хранения, калибровки/OpenRouter и платформы. Это не означает прохождения всех сценариев или временных критериев. [Сводка проверок и их границы](verification/requirements-integration-2026-10-20/combined-status-r81.json).
- **Исходная пара исторических расчётов BTC/ETH:** оба задания завершились DONE с первой попытки за 68,94 и 69,08 с. Старые ошибки не сброшены и не удалены. [Реальная очередь primary](verification/requirements-integration-2026-10-20/recovery-r81/report.json).
- **Миграция и физическое удаление старого пути:** проверены 2 212 864 исходные SQL-строки и цепочки хешей, а также 19 500 файлов старого raw-кеша. Схема `archive`, колонка `linter` и прежний путь `raw/market_frames` отсутствуют; сохранённый проверенный архив имеет срок до 19 ноября. 21 прежний reaction checkpoint неизменен; PostgreSQL остаётся на `max_locks_per_transaction=64`. [Полная сверка](verification/requirements-integration-2026-10-20/primary-r81-independent-audit.json).
- **Исправление занятого пула при native-инструментах:** 99 проверок владельца и отдельно 60 проверок на фактическом r82-образе прошли. Реальный PostgreSQL подтвердил, что 24 заблокированных внешних инструмента не удерживают пул и не останавливают восемь обычных workers; проверено восстановление после потерянного ACK. Это компонентная проверка, без модели и Telegram. [99 проверок](verification/session-pool-review-2026-10-20/report.json), [образ + PostgreSQL](verification/requirements-integration-2026-10-20/r82-image-session-pool-postgres.json), [60 проверок](verification/requirements-integration-2026-10-20/r82-native-protocol.xml).
- **Прежняя положительная доставка и восстановление:** реальный World E2E r56 доставил карточку; отдельное восстановление сверило 78 таблиц Tank, 97 таблиц OpenMA и 29 архивов скиллов. Это исторические проверки других версий. [Доставка](verification/normal-source-e2e-r56-2026-10-20/world/independent-result.json), [восстановление](verification/backup-world-r57-2026-10-20/restore-result.json).

## Текущий прогон и оставшиеся границы

- **Реальный QA r82 разрешён и запущен; PASS ещё не установлен.** Образ `aeff9822…`, код `43977ae5…`; исправлен только путь внешних инструментов SessionService. [Точный состав](verification/requirements-integration-2026-10-20/candidate-r82-runtime-tuple.json), [план реального прогона](verification/normal-source-e2e-r82-2026-10-20/prepared-manifest.json). Итог будет записан отдельно после выполнения и cleanup.
- Реальные отрицательные/неполные попытки r80/r81 сохранены. У r81 нет положительной доставки карточки; это не успешный полный E2E новой версии. [r81](verification/normal-source-e2e-r81-2026-10-20/independent-result.json).
- Полный текущий прогон всех 27 replay-сценариев, p95 на целевом Linux-хосте, 72 часа всех источников и 14 дней непрерывной работы не подтверждены. Положительный полный E2E текущей версии без X также ещё не подтверждён. X не запускался: `sources.yaml` S29 `list_id` остаётся незаданным; x86-native аппаратная приёмка отдельно не выполнена.

Jev: только OpenRouter System One, `typesafe/jev-1.13`, `jev-v1`; filter 0,23/routing 0,67 — явно одобренный оператором baseline. На 1403 сообщениях precision 47,55%, recall 61,82%, F1 53,75%: формальная цель 90/90 **не достигнута и не ослаблена**; разрешение продолжить не превращает baseline в выполненную калибровочную приёмку. [Решение](verification/prompt-selection-2026-10-20/selection.json).

Предыдущий статус r77 сохранён без изменений: [архив](verification/implementation-status-history-8b8d22a80ef5.md).
