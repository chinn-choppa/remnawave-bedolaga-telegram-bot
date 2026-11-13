"""Same-node squad switches retain accounting identity and guarded delivery."""

from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.database.models import (
    BsTrafficAllowance,
    BsTrafficLedger,
    BsTrafficRenewalIntent,
    BsTrafficUsageCursor,
    Transaction,
)
from app.services.bs_traffic import tariff_switch_checkout as checkout
from app.services.bs_traffic.ledger import grant
from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery
from tests.database.test_bs_tariff_switch_postgres import setup_switch
from tests.database.test_bs_traffic_policy_postgres import BS, ORDINARY
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
NEW_ORDINARY = '88888888-0000-4000-8000-000000000001'
NEW_BS = '88888888-0000-4000-8000-000000000002'


async def topology_setup(db, monkeypatch):
    reader, owner, sub, tariff, _ = await setup_switch(db, monkeypatch)
    tariff.allowed_squads = [NEW_ORDINARY]
    tariff.bs_traffic_settings = dict(tariff.bs_traffic_settings, squad_uuids=[NEW_BS])
    original = reader._api.get_internal_squad_topology.side_effect

    async def topology(squad):
        document = await original({NEW_ORDINARY: ORDINARY, NEW_BS: BS}.get(squad, squad))
        return dict(document, squadUuid=squad)

    reader._api.get_internal_squad_topology = AsyncMock(side_effect=topology)
    remote = {
        'state': replace(reader._api.get_split_grace_snapshot.return_value, active_internal_squads=[{'uuid': ORDINARY}])
    }
    reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])
    reader._api.get_split_access_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

    async def write(target):
        remote['state'] = target

    reader._api.restore_split_user_once = AsyncMock(side_effect=write)
    monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
    await db.commit()
    return reader, owner, sub, tariff, remote


async def request(reader, owner, tariff):
    quote = await checkout.prepare_tariff_switch(
        reader, user_id=owner['user_id'], subscription_id=owner['subscription_id'], new_tariff_id=tariff.id
    )
    return dict(
        user_id=owner['user_id'],
        subscription_id=owner['subscription_id'],
        new_tariff_id=tariff.id,
        expected_price_kopeks=quote.price_kopeks,
        expected_terms_hash=quote.terms_hash,
        operation_key=str(uuid4()),
    )


async def test_same_node_switch_delivers_and_preserves_accounting_identity(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, remote = await topology_setup(db, monkeypatch)
        now = await db.scalar(select(func.clock_timestamp()))
        topup_id = await grant(
            db,
            **owner,
            group='ordinary',
            operation_key='topup:topology',
            amount_bytes=123,
            starts_at=now,
            expires_at=sub.end_date,
        )
        ledger = await db.get(BsTrafficLedger, sub.id)
        cycle, generation, expiry = ledger.ownership_cycle, ledger.account_generation, sub.end_date
        cursors = list(await db.scalars(select(BsTrafficUsageCursor)))
        cursor_values = [tuple(getattr(row, c.name) for c in row.__table__.columns) for row in cursors]
        await db.commit()
        counter, reset_at = remote['state'].used_traffic_bytes, remote['state'].last_traffic_reset_at
        args = await request(reader, owner, tariff)
        paid = await checkout.checkout_tariff_switch(reader, **args)
        assert not paid.delivery_pending
        assert (remote['state'].used_traffic_bytes, remote['state'].last_traffic_reset_at) == (counter, reset_at)
        assert {s['uuid'] for s in remote['state'].active_internal_squads} == {NEW_ORDINARY, NEW_BS}
        assert remote['state'].expire_at == expiry
        db.expire_all()
        ledger = await db.get(BsTrafficLedger, owner['subscription_id'])
        assert (ledger.ownership_cycle, ledger.account_generation) == (cycle, generation)
        assert ledger.access_policy['topology']['ordinary_squads'] == [NEW_ORDINARY]
        topup = await db.get(BsTrafficAllowance, topup_id)
        assert (topup.remaining_bytes, topup.starts_at, topup.expires_at, topup.retired_at) == (123, now, expiry, None)
        cursors = list(await db.scalars(select(BsTrafficUsageCursor)))
        assert [tuple(getattr(row, c.name) for c in row.__table__.columns) for row in cursors] == cursor_values
        repeat = await checkout.checkout_tariff_switch(reader, **args)
        assert repeat.replayed and repeat.intent_id == paid.intent_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('fault', ['unknown', 'pending', 'review', 'grace', 'source_topology', 'target_topology'])
async def test_paid_transition_waits_without_partial_write(postgres_database, monkeypatch, fault):
    from datetime import timedelta

    from app.services.bs_traffic.ledger import open_usage

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, remote = await topology_setup(db, monkeypatch)
        args = await request(reader, owner, tariff)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        paid = await checkout.checkout_tariff_switch(reader, **args)
        before = remote['state']
        if fault == 'unknown':
            remote['state'] = replace(before, active_internal_squads=[{'uuid': str(uuid4())}])
        elif fault in ('pending', 'review'):
            db.expire_all()
            now = await db.scalar(select(func.clock_timestamp()))
            await open_usage(
                db,
                **owner,
                group='ordinary',
                source_key='pending:topology',
                amount_bytes=1,
                interval_start=now - timedelta(seconds=1),
                interval_end=now,
            )
            if fault == 'review':
                cursor = await db.scalar(
                    select(BsTrafficUsageCursor).where(BsTrafficUsageCursor.traffic_group == 'ordinary')
                )
                assert cursor is not None
                cursor.needs_review = True
        elif fault == 'grace':
            from app.database.models import Subscription

            (await db.get(Subscription, owner['subscription_id'])).grace_session_open = True
        else:
            original = reader._api.get_internal_squad_topology.side_effect

            async def drift(squad):
                document = await original(squad)
                if squad == (ORDINARY if fault == 'source_topology' else NEW_ORDINARY):
                    document = dict(
                        document, accessibleNodes=[dict(document['accessibleNodes'][0], activeInbounds=['changed'])]
                    )
                return document

            reader._api.get_internal_squad_topology.side_effect = drift
        await db.commit()
        result = await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id)
        assert result in ('obsolete', 'blocked_grace')
        reader._api.restore_split_user_once.assert_not_awaited()
        db.expire_all()
        row = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        assert row.state == 'pending' and row.attempt_token is None
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('applied', [False, True])
async def test_topology_unknown_patch_uses_readback_only(postgres_database, monkeypatch, applied):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, remote = await topology_setup(db, monkeypatch)
        args = await request(reader, owner, tariff)

        async def uncertain(target):
            if applied:
                remote['state'] = target
            raise TimeoutError

        reader._api.restore_split_user_once.side_effect = uncertain
        paid = await checkout.checkout_tariff_switch(reader, **args)
        result = await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id)
        assert result == ('verified' if applied else 'unconfirmed')
        reader._api.restore_split_user_once.assert_awaited_once()
        repeat = await checkout.checkout_tariff_switch(reader, **args)
        assert repeat.replayed and repeat.intent_id == paid.intent_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('selection', ['empty', 'shared'])
async def test_generic_delivery_cannot_skip_pending_transition(postgres_database, monkeypatch, selection):
    from app.services.bs_traffic.preparation import prepare_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, remote = await topology_setup(db, monkeypatch)
        if selection == 'shared':
            tariff.allowed_squads = [ORDINARY]
        else:
            remote['state'] = replace(remote['state'], active_internal_squads=[])
        await db.commit()
        args = await request(reader, owner, tariff)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        paid = await checkout.checkout_tariff_switch(reader, **args)
        prepared = await prepare_delivery(reader._factory, reader, **owner, operation_key='generic:transition')
        assert prepared.status == 'blocked'
        reader._api.restore_split_user_once.assert_not_awaited()
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) == 'verified'


@pytest.mark.parametrize('fault', ['fingerprint', 'added_node', 'reassigned_node', 'calendar', 'mode'])
async def test_changed_target_is_rejected_before_debit(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.ledger import LedgerConflict
    from tests.database.test_bs_traffic_policy_postgres import NODE_B

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await topology_setup(db, monkeypatch)
        args = await request(reader, owner, tariff)
        if fault == 'calendar':
            tariff.traffic_reset_mode = 'DAY'
        elif fault == 'mode':
            tariff.traffic_limit_gb = 0
        else:
            original = reader._api.get_internal_squad_topology.side_effect

            async def drift(squad):
                document = await original(squad)
                if squad == NEW_ORDINARY:
                    nodes = document['accessibleNodes']
                    node = dict(nodes[0])
                    if fault == 'fingerprint':
                        node['activeInbounds'] = ['changed']
                        nodes = [node]
                    else:
                        node['uuid'] = NODE_B if fault == 'reassigned_node' else str(uuid4())
                        nodes = [node] if fault == 'reassigned_node' else [*nodes, node]
                    document = dict(document, accessibleNodes=nodes)
                return document

            reader._api.get_internal_squad_topology.side_effect = drift
        await db.commit()
        with pytest.raises((LedgerConflict, ValueError)):
            await checkout.checkout_tariff_switch(reader, **args)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_open_grace_rejects_topology_quote_before_payment(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, _ = await topology_setup(db, monkeypatch)
        sub.grace_session_open = True
        await db.commit()
        with pytest.raises(LedgerConflict):
            await request(reader, owner, tariff)


@pytest.mark.parametrize('delivered', [False, True])
async def test_successor_switch_waits_for_transition_then_can_reverse(postgres_database, monkeypatch, delivered):
    from app.database.models import Tariff
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff, remote = await topology_setup(db, monkeypatch)
        old_id = sub.tariff_id
        args = await request(reader, owner, tariff)
        if not delivered:
            monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        first = await checkout.checkout_tariff_switch(reader, **args)
        old = await db.get(Tariff, old_id)
        await db.commit()
        if not delivered:
            with pytest.raises(LedgerConflict):
                await request(reader, owner, old)
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        else:
            reverse_args = await request(reader, owner, old)
            second = await checkout.checkout_tariff_switch(reader, **reverse_args)
            assert not second.delivery_pending
            assert {s['uuid'] for s in remote['state'].active_internal_squads} == {ORDINARY, BS}
            repeat = await checkout.checkout_tariff_switch(reader, **args)
            assert repeat.intent_id == first.intent_id and repeat.replayed
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


@pytest.mark.parametrize('fault', ['cycle', 'schema', 'pinned', 'projection', 'hash'])
async def test_corrupt_transition_receipt_cannot_authorize_panel_write(postgres_database, monkeypatch, fault):
    import hashlib
    import json

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await topology_setup(db, monkeypatch)
        args = await request(reader, owner, tariff)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='pending'))
        paid = await checkout.checkout_tariff_switch(reader, **args)
        row = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        parameters = json.loads(json.dumps(row.parameters))
        terms = parameters['switch_terms']
        value = terms['topology_transition']
        if fault == 'cycle':
            terms['ownership_cycle'] += 1
        elif fault == 'schema':
            value['source']['topology']['extra'] = []
        elif fault == 'pinned':
            value['pinned_source'] = [str(uuid4())]
        elif fault == 'projection':
            value['source_projection']['ordinary']['remaining_bytes'] = True
        else:
            value['source_projection']['revision'] += 1
        if fault != 'hash':
            parameters['switch_terms_hash'] = hashlib.sha256(
                json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()
            ).hexdigest()
        row.parameters = parameters
        await db.commit()
        assert await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id) in (
            'obsolete',
            'unconfirmed',
        )
        reader._api.restore_split_user_once.assert_not_awaited()


@pytest.mark.parametrize('fault', [None, 'successor', 'generation', 'grace', 'expiry', 'reset', 'disabled'])
async def test_expired_attempt_can_only_confirm_exact_historical_topology_target(postgres_database, monkeypatch, fault):
    from datetime import timedelta
    from types import SimpleNamespace

    from sqlalchemy import literal

    from app.database.models import Subscription
    from app.services.bs_traffic import renewal_delivery

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, remote = await topology_setup(db, monkeypatch)
        args = await request(reader, owner, tariff)

        async def uncertain(target):
            remote['state'] = target
            raise TimeoutError

        reader._api.restore_split_user_once.side_effect = uncertain
        paid = await checkout.checkout_tariff_switch(reader, **args)
        assert paid.delivery_pending
        future = remote['state'].expire_at + timedelta(seconds=1)
        remote['state'] = replace(remote['state'], status='EXPIRED')
        if fault == 'successor':
            row = await db.get(BsTrafficRenewalIntent, paid.intent_id)
            db.add(
                BsTrafficRenewalIntent(
                    subscription_id=owner['subscription_id'],
                    operation_key='synthetic:successor',
                    transaction_id=row.transaction_id,
                    charged_kopeks=row.charged_kopeks,
                    panel_scope=row.panel_scope,
                    account_generation=row.account_generation,
                    intended_expires_at=row.intended_expires_at,
                    parameters={},
                )
            )
        elif fault == 'generation':
            remote['state'] = replace(remote['state'], created_at=remote['state'].created_at + timedelta(seconds=1))
        elif fault == 'grace':
            (await db.get(Subscription, owner['subscription_id'])).grace_session_open = True
        elif fault == 'expiry':
            remote['state'] = replace(remote['state'], expire_at=remote['state'].expire_at + timedelta(seconds=1))
        elif fault == 'reset':
            remote['state'] = replace(remote['state'], last_traffic_reset_at=future)
        elif fault == 'disabled':
            remote['state'] = replace(remote['state'], status='DISABLED')
        await db.commit()
        monkeypatch.setattr(renewal_delivery, 'func', SimpleNamespace(clock_timestamp=lambda: literal(future)))
        result = await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id)
        assert (result == 'verified') is (fault is None)
        reader._api.restore_split_user_once.assert_awaited_once()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('fault', ['stale', 'fingerprint'])
async def test_core_rejects_unvalidated_target_evidence(postgres_database, monkeypatch, fault):
    from datetime import UTC, datetime, timedelta

    from app.services.bs_traffic.evidence import collect_topology_evidence
    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.tariff_switch import quote_tariff_switch

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, _, tariff, _ = await topology_setup(db, monkeypatch)
        source = await collect_topology_evidence(
            reader, panel_identity=reader.panel_scope, ordinary_squads=[ORDINARY], bs_squads=[BS]
        )
        target = await collect_topology_evidence(
            reader, panel_identity=reader.panel_scope, ordinary_squads=[NEW_ORDINARY], bs_squads=[NEW_BS]
        )
        target = replace(
            target,
            **(
                {'valid_until': datetime.now(UTC) - timedelta(seconds=1)}
                if fault == 'stale'
                else {'fingerprint': 'invalid'}
            ),
        )
        with pytest.raises(LedgerConflict):
            await quote_tariff_switch(
                db,
                **owner,
                new_tariff_id=tariff.id,
                evidence=source,
                target_evidence=target,
                current_squads=(ORDINARY,),
            )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('fault', ['pinned', 'remaining', 'nodes', 'mode'])
def test_receipt_decoder_rejects_forged_historical_permission(fault):
    import hashlib
    import json
    from types import SimpleNamespace

    from app.services.bs_traffic.ledger import LedgerConflict
    from app.services.bs_traffic.tariff_switch_topology import capture_transition, receipt_transition
    from tests.services.test_bs_traffic_squad_transition import X, values

    args = values()
    value = capture_transition(
        args['source_projection'],
        source_evidence=args['source_evidence'],
        target_evidence=args['target_evidence'],
        current_squads=args['current_squads'],
        now=args['now'],
        panel_identity=args['panel_identity'],
    )
    if fault == 'pinned':
        value['pinned_source'] = [X]
    elif fault == 'remaining':
        value['source_projection']['bs']['remaining_bytes'] = True
    elif fault == 'nodes':
        value['source']['topology']['ordinary_nodes'] = [X]
    else:
        value['source_projection']['ordinary'].update(mode='finite', remaining_bytes=10)
    terms = dict(
        account_generation='a' * 64,
        ownership_cycle=0,
        ordinary_mode='unlimited',
        bs_mode='finite',
        topology_transition=value,
    )
    parameters = dict(
        kind='tariff_switch',
        switch_terms=terms,
        switch_terms_hash=hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
    )
    row = SimpleNamespace(parameters=parameters, account_generation='a' * 64)
    with pytest.raises(LedgerConflict):
        receipt_transition(row, value['target'], ownership_cycle=0)


@pytest.mark.parametrize('selection', ['empty', 'original', 'target', 'partial', 'unknown'])
def test_inherited_renewal_manifest_authorizes_only_exact_sources(selection):
    import hashlib
    import json
    from types import SimpleNamespace

    from app.services.bs_traffic.tariff_switch_topology import capture_transition, transition_plan
    from tests.services.test_bs_traffic_squad_transition import A, B, C, D, X, values

    args = values()
    value = capture_transition(
        args['source_projection'],
        source_evidence=args['source_evidence'],
        target_evidence=args['target_evidence'],
        current_squads=args['current_squads'],
        now=args['now'],
        panel_identity=args['panel_identity'],
    )
    value.update(version=8, allow_empty_source=True)
    envelope = dict(
        version=1,
        manifest=value,
        predecessor_id=1,
        account_generation='a' * 64,
        ownership_cycle=0,
        ordinary_mode='unlimited',
        bs_mode='finite',
    )
    parameters = dict(
        topology_transition=envelope,
        topology_transition_hash=hashlib.sha256(
            json.dumps(envelope, sort_keys=True, separators=(',', ':')).encode()
        ).hexdigest(),
    )
    row = SimpleNamespace(id=2, parameters=parameters, account_generation='a' * 64)
    current = {'empty': (), 'original': (A, B), 'target': (C, D), 'partial': (A,), 'unknown': (X,)}[selection]
    result = transition_plan(
        row,
        dict(value['target'], ordinary_mode='unlimited', bs_mode='finite'),
        args['target_projection'],
        source_evidence=args['source_evidence'],
        target_evidence=args['target_evidence'],
        current_squads=current,
        now=args['now'],
        panel_identity=args['panel_identity'],
        ownership_cycle=0,
    )
    assert result.action == (
        'wait' if selection in ('partial', 'unknown') else 'unchanged' if selection == 'target' else 'patch'
    )
    assert result.squads == (None if selection in ('partial', 'unknown') else (C, D))
