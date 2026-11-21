"""Pure-Python FP-Growth for lab-scale baskets (hundreds of SKUs).
FP-Growth thuần Python cho giỏ hàng quy mô lab (vài trăm SKU).
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations
from typing import Iterable


@dataclass(frozen=True)
class AssociationRule:
    antecedents: frozenset[str]
    consequents: frozenset[str]
    support: float
    confidence: float
    lift: float
    support_count: int


class _Node:
    __slots__ = ("item", "count", "parent", "children", "link")

    def __init__(self, item: str | None, parent: "_Node | None") -> None:
        self.item = item
        self.count = 0
        self.parent = parent
        self.children: dict[str, _Node] = {}
        self.link: _Node | None = None


def mine_fpgrowth(
    baskets: Iterable[Iterable[str]],
    *,
    min_support: float = 0.01,
    min_confidence: float = 0.2,
    min_lift: float = 1.0,
    max_itemset_size: int = 3,
    max_rules: int = 200,
) -> list[AssociationRule]:
    """Mine frequent itemsets then emit association rules.
    Khai thác tập phổ biến rồi sinh luật kết hợp.
    """
    cleaned: list[frozenset[str]] = []
    for basket in baskets:
        items = frozenset(item for item in basket if item)
        if len(items) >= 2:
            cleaned.append(items)
    n = len(cleaned)
    if n == 0:
        return []

    relative = max(1, int(min_support * n + 0.999999))
    # Small samples follow relative support only; large samples keep a modest floor
    # Mẫu nhỏ chỉ theo support tương đối; mẫu lớn giữ sàn vừa phải
    min_count = max(5, relative) if n >= 500 else relative
    item_count: Counter[str] = Counter()
    for basket in cleaned:
        item_count.update(basket)
    frequent_items = {item for item, count in item_count.items() if count >= min_count}
    if not frequent_items:
        return []

    # Sort each basket by descending frequency for tree insertion
    # Sắp mỗi giỏ theo tần suất giảm dần để chèn vào cây
    ordered_baskets: list[list[str]] = []
    for basket in cleaned:
        ranked = sorted(
            (item for item in basket if item in frequent_items),
            key=lambda item: (-item_count[item], item),
        )
        if len(ranked) >= 2:
            ordered_baskets.append(ranked)

    header: dict[str, _Node | None] = {item: None for item in frequent_items}
    root = _Node(None, None)
    for basket in ordered_baskets:
        _insert(root, basket, header)

    # Collect frequent itemsets via conditional trees (size capped)
    # Thu thập tập phổ biến qua cây điều kiện (giới hạn kích thước)
    itemsets: dict[frozenset[str], int] = {
        frozenset([item]): item_count[item] for item in frequent_items
    }
    _mine_tree(root, header, frozenset(), min_count, max_itemset_size, itemsets)

    # Singleton supports for lift / Support đơn để tính lift
    singleton_support = {
        next(iter(itemset)): count / n
        for itemset, count in itemsets.items()
        if len(itemset) == 1
    }

    rules: list[AssociationRule] = []
    for itemset, count in itemsets.items():
        if len(itemset) < 2:
            continue
        support = count / n
        # Keep single-item consequents for clear "buy X → suggest Y" rules
        # Giữ hệ quả 1 item để luật dạng "mua X → gợi ý Y" rõ ràng
        for size in range(1, len(itemset)):
            for antecedents in combinations(sorted(itemset), size):
                left = frozenset(antecedents)
                right = itemset - left
                if len(right) != 1:
                    continue
                left_count = itemsets.get(left)
                if not left_count:
                    continue
                confidence = count / left_count
                if confidence < min_confidence:
                    continue
                consequent_item = next(iter(right))
                right_support = singleton_support.get(consequent_item, 0.0)
                if right_support <= 0:
                    continue
                lift = confidence / right_support
                if lift < min_lift:
                    continue
                rules.append(
                    AssociationRule(
                        antecedents=left,
                        consequents=right,
                        support=support,
                        confidence=confidence,
                        lift=lift,
                        support_count=count,
                    )
                )

    rules.sort(key=lambda rule: (-rule.lift, -rule.confidence, -rule.support))
    return rules[:max_rules]


def _insert(root: _Node, basket: list[str], header: dict[str, _Node | None]) -> None:
    node = root
    for item in basket:
        child = node.children.get(item)
        if child is None:
            child = _Node(item, node)
            node.children[item] = child
            previous = header.get(item)
            if previous is None:
                header[item] = child
            else:
                while previous.link is not None:
                    previous = previous.link
                previous.link = child
        child.count += 1
        node = child


def _mine_tree(
    root: _Node,
    header: dict[str, _Node | None],
    prefix: frozenset[str],
    min_count: int,
    max_size: int,
    out: dict[frozenset[str], int],
) -> None:
    # Process rare items first (classic FP-Growth order)
    # Xử lý item hiếm trước (thứ tự FP-Growth cổ điển)
    items = sorted(
        ((item, node) for item, node in header.items() if node is not None),
        key=lambda pair: (_header_count(pair[1]), pair[0]),
    )
    for item, head in items:
        new_prefix = prefix | {item}
        if len(new_prefix) > max_size:
            continue
        support_count = _header_count(head)
        if support_count < min_count:
            continue
        out[new_prefix] = max(out.get(new_prefix, 0), support_count)
        if len(new_prefix) >= max_size:
            continue

        # Build conditional pattern base / Dựng conditional pattern base
        conditional: Counter[tuple[str, ...]] = Counter()
        link: _Node | None = head
        while link is not None:
            path: list[str] = []
            parent = link.parent
            while parent is not None and parent.item is not None:
                path.append(parent.item)
                parent = parent.parent
            if path:
                conditional[tuple(reversed(path))] += link.count
            link = link.link
        if not conditional:
            continue

        # Conditional item frequencies / Tần suất item trong cây điều kiện
        cond_count: Counter[str] = Counter()
        for path, count in conditional.items():
            for path_item in path:
                cond_count[path_item] += count
        keep = {path_item for path_item, count in cond_count.items() if count >= min_count}
        if not keep:
            continue

        cond_root = _Node(None, None)
        cond_header: dict[str, _Node | None] = {path_item: None for path_item in keep}
        for path, count in conditional.items():
            ranked = sorted(
                (path_item for path_item in path if path_item in keep),
                key=lambda path_item: (-cond_count[path_item], path_item),
            )
            if not ranked:
                continue
            node = cond_root
            for path_item in ranked:
                child = node.children.get(path_item)
                if child is None:
                    child = _Node(path_item, node)
                    node.children[path_item] = child
                    previous = cond_header.get(path_item)
                    if previous is None:
                        cond_header[path_item] = child
                    else:
                        while previous.link is not None:
                            previous = previous.link
                        previous.link = child
                child.count += count
                node = child
        _mine_tree(cond_root, cond_header, new_prefix, min_count, max_size, out)


def _header_count(node: _Node | None) -> int:
    total = 0
    while node is not None:
        total += node.count
        node = node.link
    return total
