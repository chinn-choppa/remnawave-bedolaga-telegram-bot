"""Telegram presentation of the same owned accounting snapshot as Cabinet."""

import html
from decimal import ROUND_DOWN, Decimal

from app.services.bs_traffic.customer_summary import customer_summary
from app.services.bs_traffic.ledger import LedgerConflict


def _remaining_text(value, texts):
    amount = Decimal(value)
    if amount == 0:
        return texts.t('FORMAT_TRAFFIC_GB').format(value=0)
    if amount < 1024**2:
        return '< ' + texts.t('SETTINGS_VALUE_MB').format(value=1)
    unit, divisor = ('SETTINGS_VALUE_MB', 1024**2) if amount < 1024**3 else ('FORMAT_TRAFFIC_GB', 1024**3)
    if amount >= 1024**4:
        unit, divisor = 'FORMAT_TRAFFIC_TB', 1024**4
    rounded = (amount / divisor).quantize(Decimal('0.1'), rounding=ROUND_DOWN)
    return texts.t(unit).format(value=format(rounded, 'f').rstrip('0').rstrip('.'))


def _render_summary(data, texts):
    lines = []
    for key in ('ordinary', 'bs'):
        group = data['groups'][key]
        if group['mode'] == 'unlimited':
            value = texts.t('SPLIT_TRAFFIC_UNLIMITED')
        elif group['mode'] == 'disabled':
            value = texts.t('SPLIT_TRAFFIC_DISABLED')
        else:
            value = _remaining_text(group['remaining_bytes'], texts)
        lines.append(f'{html.escape(texts.t(f"SPLIT_TRAFFIC_{key.upper()}"))}: {html.escape(value)}')
        if group['needs_review']:
            lines.append(html.escape(texts.t('SPLIT_TRAFFIC_REVIEW')))
        elif group['pending_operations'] or group['pending_bytes'] != '0':
            lines.append(html.escape(texts.t('SPLIT_TRAFFIC_PENDING')))
    return '\n'.join(lines)


async def subscription_traffic_text(db, *, user_id, subscription, texts):
    """None preserves the legacy renderer; no writes, panel IO or activation."""
    mode = getattr(subscription, 'traffic_accounting_mode', None)
    if mode == 'split_pending':
        return html.escape(texts.t('SPLIT_TRAFFIC_TRANSITION'))
    if mode != 'split':
        return None
    try:
        data = await customer_summary(db, user_id=user_id, subscription_id=subscription.id)
    except LedgerConflict:
        return html.escape(texts.t('SPLIT_TRAFFIC_ERROR'))
    return '\n' + _render_summary(data, texts)
