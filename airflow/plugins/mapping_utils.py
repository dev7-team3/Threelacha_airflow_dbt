"""코드 매핑 데이터 로드 유틸리티

param_tree.json과 country_code.json 파일을 로드하는 함수들을 제공합니다.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Iterator, Optional

logger = logging.getLogger(__name__)

# 플러그인 디렉토리 경로
PLUGINS_DIR = Path(__file__).parent


def load_params_tree() -> Dict:
    """param_tree.json 파일을 로드합니다.

    Returns:
        dict: param_tree 데이터
    """
    param_tree_path = PLUGINS_DIR / "param_tree.json"
    with Path.open(param_tree_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_country_code_mapping() -> Dict:
    """country_code.json 파일을 로드합니다.

    Returns:
        dict: country_code 매핑 데이터
    """
    country_code_path = PLUGINS_DIR / "country_code.json"
    with Path.open(country_code_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_country_code_reverse() -> Dict:
    """country_code 매핑의 역매핑을 반환합니다.

    코드 -> 이름 매핑을 이름 -> 코드로 역변환합니다.

    Returns:
        dict: 역매핑된 country_code 딕셔너리
    """
    country_code_mapping = load_country_code_mapping()
    return {v: k for k, v in country_code_mapping.items()}


def set_category_product_variety_retail_code(params_tree: Optional[Dict] = None) -> Iterator[Dict]:
    """카테고리, 품목, 품종, 판매코드 정보를 생성합니다.

    params_tree를 순회하면서 모든 카테고리, 품목, 품종, 판매코드 조합을 생성합니다.

    Args:
        params_tree: param_tree 데이터. None인 경우 자동으로 로드합니다.

    Yields:
        dict: 카테고리, 품목, 품종, 판매코드 정보를 담은 딕셔너리
            - item_category_code: 카테고리 코드
            - item_code: 품목 코드
            - kind_code: 품종 코드
            - product_rank_code: 판매코드
    """
    if params_tree is None:
        params_tree = load_params_tree()

    for category in params_tree:
        for product in params_tree[category]["products"]:
            for variety in params_tree[category]["products"][product]["varieties"]:
                for retail_code in params_tree[category]["products"][product]["varieties"][variety]["retail_codes"]:
                    yield {
                        "item_category_code": category,
                        "item_code": product,
                        "kind_code": variety,
                        "product_rank_code": retail_code,
                    }
