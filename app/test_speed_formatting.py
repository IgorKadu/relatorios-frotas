# Testes para o helper format_speed em app.reports
# - Verifica formatação BR (separador de milhar e vírgula decimal)
# - Verifica regra de ocultação quando km_total > 0 e velocidade == 0
# - Verifica parâmetros include_unit e decimals
# - Garante tratamento de None e valores negativos

import pytest

from app.reports import format_speed


def test_hide_when_km_positive_and_speed_zero():
    """Quando km_total > 0 e velocidade == 0, deve ocultar com '—'."""
    assert format_speed(0, 100, include_unit=True, decimals=0) == '—'
    assert format_speed(0.0, 5.5, include_unit=False, decimals=2) == '—'


def test_show_zero_when_both_zero_with_and_without_unit():
    """Quando km_total == 0 e velocidade == 0, deve mostrar zero (com ou sem unidade)."""
    assert format_speed(0, 0, include_unit=True, decimals=0) == '0 km/h'
    assert format_speed(0, 0, include_unit=False, decimals=0) == '0'


def test_none_inputs_behaviour():
    """None deve ser tratado como 0. Sem km informado (None) não oculta; mostra 0."""
    assert format_speed(None, None, include_unit=True, decimals=0) == '0 km/h'
    # km=None e velocidade=0 -> não deve ocultar (regra depende de km > 0 e conhecido)
    assert format_speed(0, None, include_unit=False, decimals=0) == '0'


def test_negative_values_are_sanitized():
    """Valores negativos são tratados como 0 com a mesma regra de ocultação aplicável."""
    # velocidade negativa -> 0; km positivo -> oculta
    assert format_speed(-5, 10, include_unit=True, decimals=0) == '—'
    # km negativo -> 0; velocidade 0 -> não oculta, mostra 0
    assert format_speed(0, -10, include_unit=True, decimals=0) == '0 km/h'


def test_decimals_and_unit_formatting():
    """Verifica casas decimais e unidade com locale BR (vírgula decimal)."""
    assert format_speed(12.345, 0, include_unit=True, decimals=2) == '12,35 km/h'
    assert format_speed(12.34, 0, include_unit=True, decimals=1) == '12,3 km/h'


def test_without_unit_formatting():
    """Quando include_unit=False, não deve exibir sufixo 'km/h'."""
    assert format_speed(12.345, 0, include_unit=False, decimals=2) == '12,35'
    assert format_speed(0, 0, include_unit=False, decimals=0) == '0'


def test_thousand_separator_formatting():
    """Verifica separador de milhar no padrão brasileiro (ponto para milhar)."""
    assert format_speed(1234.56, 0, include_unit=True, decimals=1) == '1.234,6 km/h'
    assert format_speed(1234567.89, 0, include_unit=False, decimals=0) == '1.234.568'


def test_no_hide_when_distance_is_none_and_speed_zero():
    """Sem km informado (None), a regra de ocultação não se aplica; mostrar 0."""
    assert format_speed(0, None, include_unit=True, decimals=0) == '0 km/h'