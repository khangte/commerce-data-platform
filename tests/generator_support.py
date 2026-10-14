"""Generator 통합·신뢰성 테스트가 Source에 Fixture 행을 넣는 데 쓰는 트랜잭션 도우미다."""

from src.common.database import PostgresSettings
from src.generator.orders.orders import OrderBundle, OrderBundleMutationResult, persist_order_bundle
from src.generator.orders.transitions import (
    PaymentTransition,
    TransitionResult,
    persist_payment_transition,
)


def apply_order_bundle(settings: PostgresSettings, bundle: OrderBundle) -> OrderBundleMutationResult:
    """Customer·Order·Item·Payment Bundle을 하나의 Source Transaction으로 저장한다."""
    with settings.source_connection() as connection, connection.transaction():
        return persist_order_bundle(connection, bundle)


def apply_payment_transition(settings: PostgresSettings, transition: PaymentTransition) -> TransitionResult:
    """Payment 상태 전이를 하나의 Source Transaction으로 적용한다."""
    with settings.source_connection() as connection, connection.transaction():
        return persist_payment_transition(connection, transition)
