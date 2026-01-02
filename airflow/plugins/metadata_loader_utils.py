import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

logger = logging.getLogger(__name__)

WHOLESALE_CITIES = {"서울", "부산", "대구", "광주", "대전"}


class MetadataLoader:
    """메타데이터 파일 로딩 유틸"""

    # 메타데이터 디렉토리 경로
    METADATA_DIR = Path(__file__).parent / "metadata"

    @classmethod
    def _load_json(cls, filename: str) -> Dict[str, Any]:
        """
        메타데이터 디렉토리에서 JSON 파일 로드

        Args:
            filename: 파일명 (예: "country_code.json")

        Returns:
            파싱된 JSON 데이터
        """
        file_path = cls.METADATA_DIR / filename

        if not file_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {file_path}")

        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        logger.debug(f"Loaded metadata from {filename}")
        return data

    @classmethod
    def get_product_cls_codes(cls) -> Dict[str, str]:
        """
        도/소매 코드 로드

        Returns:
            {code: name} 형태
        """
        return cls._load_json("product_cls_code.json")

    @classmethod
    def get_category_codes(cls) -> Dict:
        """
        부류 코드 로드

        Returns:
            {code: name} 형태
        """
        return cls._load_json("category_code.json")

    @classmethod
    def get_country_codes(cls, wholesale_only: bool = False) -> Dict[str, str]:
        """
        지역 코드 로드

        Args:
            wholesale_only: True이면 도매 주요 광역시만 (서울, 부산, 대구, 광주, 대전)

        Returns:
            {code: name} 형태
        """
        # 전체 지역 정보 로드
        all_country_info = cls._load_json("retail_country_code.json")

        if wholesale_only:
            # 도매 주요 광역시만 필터링
            return {code: name for code, name in all_country_info.items() if name in WHOLESALE_CITIES}

        return all_country_info

    @classmethod
    def get_param_tree(cls) -> Dict:
        """
        부류-품목-품종-등급(도매/소매/친환경) 트리 로드

        Returns:
            계층 구조 딕셔너리
        """
        return cls._load_json("param_tree.json")


# ============================================================
# API별 파라미터 생성 함수
# ============================================================


def generate_api1_params() -> Iterator[Dict[str, Optional[str]]]:
    """
    API1 파라미터 조합 생성

    Yields:
        {product_cls_code, category_code, country_code}
        - product_cls_code: "01" (소매만)
        - category_code: "100"~"600"
        - country_code: 지역 코드 또는 None (전체)
    """
    from itertools import product

    # 메타데이터에서 로드
    product_cls_codes = MetadataLoader.get_product_cls_codes()
    category_codes = MetadataLoader.get_category_codes().keys()
    country_codes = MetadataLoader.get_country_codes(wholesale_only=True)

    # 소매("01")만 필터링
    target_cls_codes = [code for code in product_cls_codes.keys() if code == "01"]  # noqa: SIM118

    # 지역 코드 + None (전체 지역)
    target_country_codes = list(country_codes.keys()) + [None]  # noqa: RUF005
    logger.info(
        f"API1 파라미터 생성: {len(target_cls_codes)} 분류 × {len(category_codes)} 카테고리 × {len(target_country_codes)} 지역"  # noqa: RUF001
    )
    logger.info(f"target_country_codes: {target_country_codes}")

    for p_cls, category, country in product(
        target_cls_codes,
        category_codes,
        target_country_codes,
    ):
        yield {
            "product_cls_code": p_cls,
            "category_code": category,
            "country_code": country,
        }


def generate_api10_params() -> Iterator[Dict[str, str]]:
    """
    API10 파라미터 조합 생성 (소매 지역)

    Yields:
        {country_code}
        - country_code: 소매 지역 코드 (24개)
    """
    countries = MetadataLoader.get_country_codes(wholesale_only=False)

    logger.debug(f"API10 파라미터 생성: {len(countries)} 지역")

    for country_code in countries.keys():  # code 사용  # noqa: SIM118
        yield {"country_code": country_code}


def generate_api17_params() -> Iterator[Dict[str, str]]:
    """
    API17 파라미터 조합 생성

    Yields:
        {item_category_code, item_code, kind_code, product_rank_code}
    """
    param_tree = MetadataLoader.get_param_tree()

    total = 0
    for category_code, cat_data in param_tree.items():
        for product_code, prod_data in cat_data["products"].items():
            for variety_code, var_data in prod_data["varieties"].items():
                for rank_code in var_data["retail_codes"]:
                    total += 1
                    yield {
                        "item_category_code": category_code,
                        "item_code": product_code,
                        "kind_code": variety_code,
                        "product_rank_code": rank_code,
                    }

    logger.debug(f"API17 파라미터 생성: {total} 조합")
