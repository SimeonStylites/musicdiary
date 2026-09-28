# musicdiary — памятка для ИИ-агента

Пайплайн: Spotify JSON / API → Postgres → CSV → HTML-дашборд → GitHub Pages.
Живой сайт: https://simeonstylites.github.io/musicdiary/

## Стек

Python + `psycopg2` (Postgres) + `spotipy` + `pandas`. Секреты и `DATABASE_URL` — в `.env`
(в git не попадает, `.env` в `.gitignore`). Всё запускается из venv проекта:
`venv\Scripts\python.exe` — обязательно, не системный `python`.

## Запуск

```powershell
venv\Scripts\python.exe pipeline.py                     # все шаги по умолчанию
venv\Scripts\python.exe pipeline.py import enrich-spotify export dashboard push
```

## Шаги pipeline.py

| Шаг | Что делает |
|---|---|
| `import` | JSON из `DATA_FOLDER` → `listening_events`, дедуп по `played_at` |
| `collect` | последние 50 треков из Spotify API → БД. Пишет сразу в spotify-колонки |
| `enrich-spotify` | добирает `spotify_release_date`/`spotify_total_tracks` для альбомов с событиями, порциями по 100 |
| `enrich-mb` | MusicBrainz: mbid, алиасы. Для дашборда не нужен, ходит по сети долго |
| `export` | SQL → `album_full_plays.csv`, только альбомы где прослушаны ВСЕ треки и их ≥3 |
| `dashboard` | CSV + `album_dashboard_template.html` → `album_dashboard.html` и `docs/index.html` |
| `push` | коммит + пуш только если дашборд реально изменился |

`DATA_FOLDER` (pipeline.py:16) указывает на `my_spotify_data_3/Spotify Extended Streaming History`.
При новом экспорте из Spotify: распаковать zip, поменять `DATA_FOLDER`, добавить папку в `.gitignore`.

## Схема БД — важная ловушка

В таблице `albums` **две пары колонок**:

- `total_tracks` / `release_date` — legacy, их пишет только `collect.py`
- `spotify_total_tracks` / `spotify_release_date` — их читает `enrich` и `export`

`collect.py` пишет в первые, а `export` смотрит на вторые. Из-за этого новый альбом,
созданный `collect.py`, в дашборд не попадёт, пока не отработает `enrich-spotify`.
Поэтому для автоматизации используется `pipeline.py collect`, а не `collect.py`.

## Грабли, уже зашитые

- **cp1251 в консоли.** Раньше падало `UnicodeEncodeError` на стрелке `→`. Починено в `_init_logging()` — stdout перенастраивается на UTF-8.
- **`enrich-spotify` застревал.** `LIMIT 100` без фильтра по наличию событий возвращал одни и те же 100 альбомов, из которых 104 вовсе не имели прослушиваний (`continue`) — цикл крутился впустую. Починено через `EXISTS` по `listening_events`.
- **`step_generate_dashboard` не принимал `conn`**, а `main` зовёт все шаги с ним — шаг падал с TypeError. Починено: `def step_generate_dashboard(conn=None)`.
- **`release_date: "0000"`** от Spotify (сборники Various Artists с пустым именем). `normalize_release_date` такое отбрасывает. Два таких альбома (Chicherina «Сны», Krasnaya Plesen «Девятый бред») поправлены вручную из открытых источников; см. ниже.

## Логи

`pipeline.log` в корне, с метками времени, в `.gitignore`. Ротации нет — растёт ~15 строк в день.

## Задача в расписании Windows

`MusicDiary ежедневно`, триггер 15:00, `LogonType: Password` (работает без залогна),
`RunLevel: Highest`.

**НЕ ДОДЕЛАНО.** Задача до сих пор запускает `collect.py`, а не `pipeline.py` с шагом `push`.
Переключение упирается в `Set-ScheduledTask` → `0x80070005` (нужны права админа).
Нужно выполнить в PowerShell от администратора:

```powershell
$t = Get-ScheduledTask | Where-Object { $_.TaskName -like "MusicDiary*" }
Export-ScheduledTask -TaskName $t.TaskName | Out-File "$env:TEMP\MusicDiary_backup.xml"
$a = New-ScheduledTaskAction -Execute "C:\Users\Дмитрий\Projects\musicdiary\venv\Scripts\python.exe" `
     -Argument "pipeline.py collect enrich-spotify export dashboard push" `
     -WorkingDirectory "C:\Users\Дмитрий\Projects\musicdiary"
$s = $t.Settings; $s.DisallowStartIfOnBatteries = $false; $s.StopIfGoingOnBatteries = $false
Set-ScheduledTask -TaskName $t.TaskName -Action $a -Settings $s
```

Проверка: `Get-Content pipeline.log -Tail 20`, либо `Start-ScheduledTask` и лог через минуту.

`gh` CLI не установлен. Креды для push лежат в Windows Credential Manager
(`git:https://github.com`, пользователь SimeonStylites) — неинтерактивный push работает.

## GitHub Pages

- В UI для «Deploy from a branch» можно выбрать только `/(root)` или `/docs` — свою папку нельзя.
  Поэтому папка `github-pages` переименована в `docs`.
- Pages включён: ветка `main`, папка `/docs`.

## Текущее состояние данных

- 21846 событий, 3816 альбомов, 104 альбома без единого прослушивания (технический мусор)
- Альбомов с прослушиваниями, но без даты релиза: **0**
- 155 полностью прослушанных альбомов, 120 артистов, 295 прослушиваний, 1954–2026

## Ручные правки дат (не из Spotify)

| Альбом | Дата | Треков | Источник |
|---|---|---|---|
| Chicherina — Сны | 2000-07-25 | 11 | Wikipedia, Apple Music |
| Krasnaya Plesen — Девятый бред | 1994-08-25 | 28 | Deezer |

Оба всё равно не попадают в дашборд: прослушан 1 трек из 11 и из 28, а фильтр требует все.

## Яндекс.Музыка — обсуждалось, не начато

Аналог пайплайна возможен, но с оговорками:
- официального API нет, только неофициальная библиотека `yandex-music-api` (MarshalX), авторизация по токену Яндекс.Паспорта
- файла-экспорта нет; бэкфилл возможен через архив `id.yandex.ru/personal/data` → `history.json`
- `music_history` в API — не полный лог проигрываний, надо проверять на реальных данных

## Стиль правок

Пользователь просит точечных изменений, без «заодно» рефакторить. Перед коммитом — показать
список изменений и отдельно то, что сделано по своей инициативе. Не выдумывать результаты:
что не проверено — говорить прямо.
