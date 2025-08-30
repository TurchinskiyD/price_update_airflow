#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ubuntu/airflow-project/price_update_airflow"
VENV_DIR="$APP_DIR/venv"
AIRFLOW_DAGS_DIR="/home/ubuntu/airflow/dags"
BRANCH="${BRANCH:-main}"

echo ">>> [1/4] Git pull"
git -C "$APP_DIR" fetch --all
git -C "$APP_DIR" checkout "$BRANCH"
git -C "$APP_DIR" reset --hard "origin/$BRANCH"

echo ">>> [2/4] Активуємо venv і ставимо залежності"
source "$VENV_DIR/bin/activate"
if git -C "$APP_DIR" diff --name-only HEAD@{1} HEAD | grep -q "requirements.txt"; then
  echo "requirements.txt змінився → оновлюємо пакети"
  source "$VENV_DIR/bin/activate"
  pip install -U pip
  pip install -r "$APP_DIR/requirements.txt"
else
  echo "requirements.txt не змінювався → пропускаємо оновлення пакетів"
fi

echo ">>> [3/3] Синхронізація симлінків DAGs"
cd "$APP_DIR/dags"
for dag_file in *.py; do
  if [ ! -L "$AIRFLOW_DAGS_DIR/$dag_file" ]; then
    ln -s "$APP_DIR/dags/$dag_file" "$AIRFLOW_DAGS_DIR/$dag_file"
    echo "Створено симлінк для $dag_file"
  fi
done

# видаляємо старі симлінки, якщо DAG видалений у проєкті
for link in "$AIRFLOW_DAGS_DIR"/*.py; do
  target=$(readlink "$link" || true)
  if [ -n "$target" ] && [[ "$target" == "$APP_DIR/dags/"* ]] && [ ! -f "$target" ]; then
    rm "$link"
    echo "Видалено симлінк $link (файл видалений у проєкті)"
  fi
done

echo ">>> Done (price_update_airflow)"
