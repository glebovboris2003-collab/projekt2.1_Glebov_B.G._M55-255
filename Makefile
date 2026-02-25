# Makefile для управления проектом Primitive DB

.PHONY: install project lint format build publish package-install clean

install:
	@echo "Установка зависимостей проекта через Poetry..."
	poetry install

project:
	@echo "Запуск базы данных..."
	poetry run project

lint:
	@echo "Запуск проверки кода (Ruff)..."
	poetry run ruff check src/

format:
	@echo "Автоформатирование кода (Ruff)..."
	poetry run ruff format src/

build:
	@echo "Сборка пакета (Wheel/Tarball)..."
	poetry build

publish: build
	@echo "Публикация пакета..."
	poetry publish

package-install:
	@echo "Установка собранного пакета в систему..."
	python3 -m pip install --upgrade pip
	python3 -m pip install --force-reinstall dist/*.whl

clean:
	@echo "Очистка временных файлов и кэша..."
	rm -rf dist/ build/ .ruff_cache/ .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} +