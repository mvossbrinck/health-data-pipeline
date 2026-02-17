"""
STEP 3: VISUALIZE & DELIVER
============================
This script queries the transformed mart tables and creates 
visualizations. In production, this step might be replaced by 
Looker, Tableau, Metabase, or Hex — but the principle is the same:
query clean, transformed data and present insights.
"""

import duckdb
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import pandas as pd

DB_PATH = "healthcare.duckdb"
conn = duckdb.connect(DB_PATH, read_only=True)

# ── QUERY THE MART ──────────────────────────────────────────────────────
print("=" * 60)
print("STEP 3: QUERYING TRANSFORMED DATA & CREATING VISUALIZATIONS")
print("=" * 60)

# Pull state-level summary from our dbt mart
df_states = conn.execute("""
    SELECT * FROM analytics.mart_state_hospital_summary
    ORDER BY total_hospitals DESC
""").fetchdf()

# Pull hospital-level quality data
df_hospitals = conn.execute("""
    SELECT * FROM analytics.mart_hospital_quality
""").fetchdf()

print(f"\n  State summary: {len(df_states)} states")
print(f"  Hospital detail: {len(df_hospitals):,} hospitals")

# ── CHART 1: Hospitals by State ─────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('CMS Hospital Data Pipeline — Dashboard', fontsize=16, fontweight='bold')

# Top 15 states by hospital count
top_states = df_states.head(15)
colors = ['#2196F3' if t == 'Above Average' else '#FF9800' if t == 'Average' else '#F44336' 
          for t in top_states['quality_tier']]
axes[0, 0].barh(top_states['state'][::-1], top_states['total_hospitals'][::-1], color=colors[::-1])
axes[0, 0].set_title('Hospital Count by State (Top 15)', fontweight='bold')
axes[0, 0].set_xlabel('Number of Hospitals')

# Add legend
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='#2196F3', label='Above Avg Rating'),
    Patch(facecolor='#FF9800', label='Average Rating'),
    Patch(facecolor='#F44336', label='Below Avg Rating'),
]
axes[0, 0].legend(handles=legend_elements, loc='lower right', fontsize=8)

# ── CHART 2: Rating Distribution ────────────────────────────────────────
rating_dist = df_hospitals['overall_rating'].value_counts().sort_index()
rated = rating_dist[rating_dist.index.notna()]
bar_colors = ['#F44336', '#FF9800', '#FFC107', '#8BC34A', '#4CAF50']
axes[0, 1].bar(rated.index.astype(int), rated.values, color=bar_colors)
axes[0, 1].set_title('Hospital Rating Distribution', fontweight='bold')
axes[0, 1].set_xlabel('CMS Star Rating')
axes[0, 1].set_ylabel('Number of Hospitals')
axes[0, 1].set_xticks([1, 2, 3, 4, 5])

# ── CHART 3: Emergency Services by Quality ──────────────────────────────
quality_groups = df_hospitals.groupby('quality_classification').agg(
    total=('facility_id', 'count'),
    with_emergency=('has_emergency_services', 'sum')
).reset_index()
quality_groups['pct_emergency'] = 100 * quality_groups['with_emergency'] / quality_groups['total']

# Order logically
order = ['High Quality', 'Average', 'Needs Improvement', 'Not Rated']
quality_groups['sort_key'] = quality_groups['quality_classification'].map(
    {v: i for i, v in enumerate(order)}
)
quality_groups = quality_groups.sort_values('sort_key')

q_colors = ['#4CAF50', '#FFC107', '#F44336', '#9E9E9E']
axes[1, 0].bar(quality_groups['quality_classification'], 
               quality_groups['pct_emergency'], color=q_colors)
axes[1, 0].set_title('% with Emergency Services by Quality', fontweight='bold')
axes[1, 0].set_ylabel('% of Hospitals')
axes[1, 0].set_ylim(0, 100)
axes[1, 0].tick_params(axis='x', rotation=15)

# ── CHART 4: Ownership Breakdown ────────────────────────────────────────
ownership_dist = df_hospitals['ownership_category'].value_counts()
own_colors = ['#2196F3', '#FF9800', '#4CAF50', '#9E9E9E']
axes[1, 1].pie(ownership_dist, labels=ownership_dist.index, autopct='%1.0f%%',
               colors=own_colors, startangle=90)
axes[1, 1].set_title('Hospital Ownership Breakdown', fontweight='bold')

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig('dashboard.png', dpi=150, bbox_inches='tight')
print(f"\n  ✓ Dashboard saved: dashboard.png")

# ── PRINT KEY INSIGHTS ──────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print("KEY INSIGHTS FROM THE PIPELINE")
print(f"{'=' * 60}")

avg_rating = df_hospitals['overall_rating'].mean()
print(f"\n  Overall avg hospital rating: {avg_rating:.2f} / 5.0")

best_state = df_states.loc[df_states['avg_rating'].idxmax()]
print(f"  Highest-rated state: {best_state['state']} (avg {best_state['avg_rating']:.2f})")

worst_state = df_states.loc[df_states['avg_rating'].idxmin()]
print(f"  Lowest-rated state:  {worst_state['state']} (avg {worst_state['avg_rating']:.2f})")

access_risk = df_hospitals['access_risk_flag'].sum()
print(f"  Hospitals flagged for access risk: {access_risk}")

ehr_avg = df_states['pct_ehr_interop'].mean()
print(f"  Avg EHR interoperability rate: {ehr_avg:.1f}%")

conn.close()

print(f"\n{'=' * 60}")
print("✓ PIPELINE COMPLETE: Extract → Load → Transform → Visualize")
print(f"{'=' * 60}")
