"""Analytics pipeline steps.
Các bước pipeline analytics.
"""

from jobs.kaizen_analytics.steps.association_rules import AssociationRulesStep
from jobs.kaizen_analytics.steps.customer_segments import CustomerSegmentsStep
from jobs.kaizen_analytics.steps.replenishment_plan import ReplenishmentPlanStep

DEFAULT_STEPS = (
    CustomerSegmentsStep(),
    AssociationRulesStep(),
    ReplenishmentPlanStep(),
)

__all__ = [
    "AssociationRulesStep",
    "CustomerSegmentsStep",
    "ReplenishmentPlanStep",
    "DEFAULT_STEPS",
]
