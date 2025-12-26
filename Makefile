.PHONY: fetch prices build figs h1 h2 h3 macros analysis frontend all

fetch:
	python -m src.etl.pull_fcd

prices:
	python -m src.etl.fetch_prices

build:
	python -m src.etl.build_panel

figs:
	python -m src.analysis.descriptive

h1:
	python -m src.analysis.hazard

h2:
	python -m src.analysis.event_study

h3:
	python -m src.analysis.losses

macros:
	python -m src.analysis.report_macros

analysis: h1 h2 h3 macros

frontend:
	cp data/processed/flows_hourly.csv frontend/assets/flows_hourly.csv

all: fetch prices build figs analysis frontend
