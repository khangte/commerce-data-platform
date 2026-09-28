"""Generator가 원천 커서 계약을 위반할 때 사용하는 예외를 정의한다."""


class SourceCursorRegressionError(RuntimeError):
    """새 원천 행의 갱신 시각이 기존 최대 시각을 넘지 못할 때 발생한다."""
