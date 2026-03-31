#!/usr/bin/env python3
"""
Generate GPU topology diagrams for Ares Scheduler documentation.

Produces 3 PNGs:
  1. NVSwitch full crossbar topology (p4d.24xlarge, 8x A100)
  2. PCIe-based topology (p3.8xlarge, 4x V100)
  3. NCCL benchmark results bar chart

Usage:
  python scripts/generate_topology_diagrams.py
"""

import os
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'docs', 'diagrams')

# Color palette
NUMA0_COLOR = '#4A90D9'      # Blue - NUMA domain 0
NUMA1_COLOR = '#E8833A'      # Orange - NUMA domain 1
NVSWITCH_COLOR = '#2ECC71'   # Green - NVSwitch fabric
NVLINK_COLOR = '#27AE60'     # Dark green - NVLink connections
PCIE_COLOR = '#E74C3C'       # Red - PCIe connections
BG_COLOR = '#FAFAFA'         # Light background
TEXT_COLOR = '#2C3E50'        # Dark text
GRID_COLOR = '#ECF0F1'       # Light grid
BAR_COLOR_SAME = '#4A90D9'   # Blue bars (same NUMA)
BAR_COLOR_CROSS = '#E8833A'  # Orange bars (cross NUMA)
BAR_COLOR_ALL = '#9B59B6'    # Purple bars (all GPUs)


def generate_nvswitch_crossbar():
    """Diagram A: NVSwitch full crossbar topology - p4d.24xlarge."""

    fig, ax = plt.subplots(1, 1, figsize=(12, 12))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(-1.8, 1.8)
    ax.set_ylim(-2.1, 2.1)
    ax.set_aspect('equal')
    ax.axis('off')

    # Title
    ax.text(0, 1.95, 'NVSwitch Full Crossbar Topology',
            ha='center', va='top', fontsize=20, fontweight='bold', color=TEXT_COLOR)
    ax.text(0, 1.78, 'p4d.24xlarge  |  8x A100-SXM4-40GB  |  6x NVSwitch',
            ha='center', va='top', fontsize=13, color='#7F8C8D')

    # Place 8 GPUs in a circle
    n_gpus = 8
    radius = 1.15
    gpu_positions = []
    for i in range(n_gpus):
        angle = math.pi / 2 - (2 * math.pi * i / n_gpus)  # start from top
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        gpu_positions.append((x, y))

    # Draw NVSwitch fabric in center
    nvswitch_box = FancyBboxPatch(
        (-0.42, -0.22), 0.84, 0.44,
        boxstyle="round,pad=0.08",
        facecolor=NVSWITCH_COLOR, edgecolor='#1B8A4A',
        alpha=0.25, linewidth=2
    )
    ax.add_patch(nvswitch_box)
    ax.text(0, 0.05, 'NVSwitch', ha='center', va='center',
            fontsize=14, fontweight='bold', color='#1B8A4A')
    ax.text(0, -0.12, '6x NVSwitch ASICs', ha='center', va='center',
            fontsize=10, color='#1B8A4A', style='italic')

    # Draw all 28 NVLink connections (full mesh) — faint lines through center
    for i in range(n_gpus):
        for j in range(i + 1, n_gpus):
            x1, y1 = gpu_positions[i]
            x2, y2 = gpu_positions[j]
            ax.plot([x1, x2], [y1, y2],
                    color=NVLINK_COLOR, alpha=0.12, linewidth=1.5,
                    zorder=1)

    # Draw GPU nodes
    gpu_radius = 0.14
    for i, (x, y) in enumerate(gpu_positions):
        color = NUMA0_COLOR if i < 4 else NUMA1_COLOR
        circle = plt.Circle((x, y), gpu_radius, facecolor=color,
                            edgecolor='white', linewidth=3, zorder=5)
        ax.add_patch(circle)
        ax.text(x, y + 0.01, f'GPU {i}', ha='center', va='center',
                fontsize=11, fontweight='bold', color='white', zorder=6)

    # Draw NV12 label on a few representative connections
    pairs_to_label = [(0, 4), (1, 6), (3, 5)]
    for i, j in pairs_to_label:
        x1, y1 = gpu_positions[i]
        x2, y2 = gpu_positions[j]
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        # Offset label slightly from center
        dx, dy = mx * 0.15, my * 0.15
        ax.annotate('NV12', xy=(mx + dx, my + dy),
                    fontsize=8, color=NVLINK_COLOR, fontweight='bold',
                    ha='center', va='center',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                              edgecolor=NVLINK_COLOR, alpha=0.85))

    # NUMA domain legends
    numa0_patch = mpatches.Patch(color=NUMA0_COLOR, label='NUMA 0 (GPU 0-3, CPU 0-23)')
    numa1_patch = mpatches.Patch(color=NUMA1_COLOR, label='NUMA 1 (GPU 4-7, CPU 24-47)')
    ax.legend(handles=[numa0_patch, numa1_patch], loc='lower center',
              fontsize=11, framealpha=0.9, ncol=2,
              bbox_to_anchor=(0.5, -0.02))

    # Key finding annotation box
    bbox_props = dict(boxstyle="round,pad=0.6", facecolor='#FFF9C4',
                      edgecolor='#F9A825', alpha=0.95, linewidth=2)
    ax.text(0, -1.55,
            'NCCL Benchmark Result: < 1% bandwidth variance\n'
            'across all GPU placements (39.9 - 40.2 GB/s)\n\n'
            'Every GPU pair has 12 NVLink bridges (~200 GB/s)\n'
            'Intra-node topology optimization is unnecessary',
            ha='center', va='center', fontsize=11, color=TEXT_COLOR,
            bbox=bbox_props, linespacing=1.5)

    # Connection count annotation
    ax.text(0, -2.0, '28 GPU pairs  x  12 NVLink bridges each  =  full mesh',
            ha='center', va='center', fontsize=10, color='#95A5A6', style='italic')

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, 'gpu-nvswitch-crossbar.png')
    fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=BG_COLOR)
    plt.close(fig)
    print(f"  Created: {path}")


def generate_pcie_topology():
    """Diagram B: PCIe-based topology - p3.8xlarge (4x V100)."""

    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)
    ax.set_xlim(-0.5, 13.5)
    ax.set_ylim(-1.5, 9.5)
    ax.axis('off')

    # Title
    ax.text(6.5, 9.2, 'PCIe-Based Topology (No NVSwitch)',
            ha='center', va='top', fontsize=20, fontweight='bold', color=TEXT_COLOR)
    ax.text(6.5, 8.7, 'p3.8xlarge  |  4x V100-SXM2-16GB  |  Partial NVLink',
            ha='center', va='top', fontsize=13, color='#7F8C8D')

    def draw_box(ax, x, y, w, h, label, color, sublabel=None, fontsize=12):
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1",
                             facecolor=color, edgecolor='white', linewidth=2.5, alpha=0.9)
        ax.add_patch(box)
        ax.text(x + w/2, y + h/2 + (0.1 if sublabel else 0), label,
                ha='center', va='center', fontsize=fontsize,
                fontweight='bold', color='white', zorder=10)
        if sublabel:
            ax.text(x + w/2, y + h/2 - 0.2, sublabel,
                    ha='center', va='center', fontsize=9,
                    color=(1, 1, 1, 0.8), zorder=10)

    # --- Layer 1: CPUs ---
    draw_box(ax, 1.5, 7, 3, 0.9, 'CPU 0', '#34495E', 'NUMA Domain 0')
    draw_box(ax, 8.5, 7, 3, 0.9, 'CPU 1', '#34495E', 'NUMA Domain 1')

    # --- Layer 2: PCIe Root Complex ---
    draw_box(ax, 1.8, 5.3, 2.4, 0.7, 'PCIe Root', '#7F8C8D', fontsize=11)
    draw_box(ax, 9.0, 5.3, 2.4, 0.7, 'PCIe Root', '#7F8C8D', fontsize=11)

    # CPU to PCIe root connections
    ax.annotate('', xy=(3, 5.95), xytext=(3, 7.0),
                arrowprops=dict(arrowstyle='->', color='#7F8C8D', lw=2))
    ax.annotate('', xy=(10.2, 5.95), xytext=(10.2, 7.0),
                arrowprops=dict(arrowstyle='->', color='#7F8C8D', lw=2))

    # --- Layer 3: PCIe Switches ---
    draw_box(ax, 0.8, 3.5, 1.8, 0.6, 'PCIe Sw', '#95A5A6', fontsize=10)
    draw_box(ax, 3.4, 3.5, 1.8, 0.6, 'PCIe Sw', '#95A5A6', fontsize=10)
    draw_box(ax, 8.0, 3.5, 1.8, 0.6, 'PCIe Sw', '#95A5A6', fontsize=10)
    draw_box(ax, 10.6, 3.5, 1.8, 0.6, 'PCIe Sw', '#95A5A6', fontsize=10)

    # PCIe root to switches
    for x in [1.7, 4.3]:
        ax.plot([3, x], [5.3, 4.1], color='#95A5A6', lw=1.5, ls='--')
    for x in [8.9, 11.5]:
        ax.plot([10.2, x], [5.3, 4.1], color='#95A5A6', lw=1.5, ls='--')

    # --- Layer 4: GPUs ---
    gpu_x = [1.2, 3.8, 8.4, 11.0]
    gpu_labels = ['GPU 0', 'GPU 1', 'GPU 2', 'GPU 3']
    gpu_colors = [NUMA0_COLOR, NUMA0_COLOR, NUMA1_COLOR, NUMA1_COLOR]

    for i, (x, label, color) in enumerate(zip(gpu_x, gpu_labels, gpu_colors)):
        draw_box(ax, x, 1.8, 1.6, 0.8, label, color, 'V100-SXM2')

    # PCIe switch to GPU connections (dashed = PCIe)
    switch_x = [1.7, 4.3, 8.9, 11.5]
    for sx, gx in zip(switch_x, gpu_x):
        ax.plot([sx, gx + 0.8], [3.5, 2.6], color=PCIE_COLOR, lw=1.5,
                ls='--', alpha=0.6)

    # NVLink bridge between GPU 0 <-> GPU 1 (GOOD placement)
    ax.annotate('',
                xy=(3.8, 2.3), xytext=(2.8, 2.3),
                arrowprops=dict(arrowstyle='<->', color=NVLINK_COLOR,
                                lw=3.5, connectionstyle='arc3,rad=-0.3'))
    ax.text(3.3, 1.35, 'NVLink\n~200 GB/s',
            ha='center', va='center', fontsize=10, fontweight='bold',
            color=NVLINK_COLOR,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F5E9',
                      edgecolor=NVLINK_COLOR, alpha=0.9))

    # NVLink bridge between GPU 2 <-> GPU 3
    ax.annotate('',
                xy=(11.0, 2.3), xytext=(10.0, 2.3),
                arrowprops=dict(arrowstyle='<->', color=NVLINK_COLOR,
                                lw=3.5, connectionstyle='arc3,rad=-0.3'))
    ax.text(10.5, 1.35, 'NVLink\n~200 GB/s',
            ha='center', va='center', fontsize=10, fontweight='bold',
            color=NVLINK_COLOR,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F5E9',
                      edgecolor=NVLINK_COLOR, alpha=0.9))

    # Cross-NUMA PCIe connection (BAD placement) GPU 1 <-> GPU 2
    mid_x = (5.4 + 8.4) / 2
    ax.annotate('',
                xy=(8.4, 2.2), xytext=(5.4, 2.2),
                arrowprops=dict(arrowstyle='<->', color=PCIE_COLOR,
                                lw=2, ls='--',
                                connectionstyle='arc3,rad=-0.25'))
    ax.text(mid_x, 0.85, 'PCIe Only\n~32 GB/s',
            ha='center', va='center', fontsize=10, fontweight='bold',
            color=PCIE_COLOR,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='#FFEBEE',
                      edgecolor=PCIE_COLOR, alpha=0.9))

    # Good/Bad placement annotations
    # Good
    ax.text(3.3, 0.35, 'GOOD placement\nGPU 0 + GPU 1 (NVLink)',
            ha='center', va='center', fontsize=11, fontweight='bold',
            color=NVLINK_COLOR,
            bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                      edgecolor=NVLINK_COLOR, alpha=0.9, linewidth=2))

    # Bad
    ax.text(mid_x, -0.15, 'BAD placement\nGPU 1 + GPU 2 (PCIe cross-NUMA)\n6.25x slower',
            ha='center', va='center', fontsize=11, fontweight='bold',
            color=PCIE_COLOR,
            bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                      edgecolor=PCIE_COLOR, alpha=0.9, linewidth=2))

    # Key finding box
    bbox_props = dict(boxstyle="round,pad=0.5", facecolor='#FFF9C4',
                      edgecolor='#F9A825', alpha=0.95, linewidth=2)
    ax.text(6.5, -1.1,
            'On PCIe-based hardware, GPU placement matters: '
            'NVLink pairs get ~200 GB/s, PCIe pairs get ~32 GB/s (5-37x slower).\n'
            'Ares topology scorer optimizes for NVLink affinity on these systems.',
            ha='center', va='center', fontsize=10.5, color=TEXT_COLOR,
            bbox=bbox_props, linespacing=1.4)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, 'gpu-pcie-topology.png')
    fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=BG_COLOR)
    plt.close(fig)
    print(f"  Created: {path}")


def generate_nccl_benchmark():
    """Diagram C: NCCL benchmark results bar chart."""

    fig, ax = plt.subplots(1, 1, figsize=(13, 7))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    # Data from NCCL benchmarks on p4d.24xlarge
    tests = [
        'Same NUMA\n(GPU 0,1)',
        'Cross NUMA\n(GPU 0,4)',
        'Same NUMA\n(GPU 0,1,2,3)',
        'Cross NUMA\n(GPU 0,1,4,5)',
        '8-GPU\nSequential',
        '8-GPU\nInterleaved',
    ]
    avg_bw = [39.9, 40.2, 46.2, 46.7, 45.1, 44.9]
    peak_bw = [179, 180, 206, 206, 215, 214]
    gpu_counts = [2, 2, 4, 4, 8, 8]
    colors = [BAR_COLOR_SAME, BAR_COLOR_CROSS,
              BAR_COLOR_SAME, BAR_COLOR_CROSS,
              BAR_COLOR_ALL, BAR_COLOR_ALL]

    x = np.arange(len(tests))
    bar_width = 0.55

    bars = ax.bar(x, avg_bw, bar_width, color=colors, edgecolor='white',
                  linewidth=2, alpha=0.9, zorder=3)

    # Value labels on bars
    for bar, val, peak, gpus in zip(bars, avg_bw, peak_bw, gpu_counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f'{val} GB/s', ha='center', va='bottom',
                fontsize=12, fontweight='bold', color=TEXT_COLOR)
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() - 2,
                f'peak: {peak} GB/s', ha='center', va='top',
                fontsize=9, color='white', style='italic')

    # Reference line at average
    mean_bw = np.mean(avg_bw)
    ax.axhline(y=mean_bw, color='#E74C3C', linestyle='--', linewidth=1.5,
               alpha=0.7, zorder=2)
    ax.text(len(tests) - 0.5, mean_bw + 0.5, f'avg: {mean_bw:.1f} GB/s',
            ha='right', va='bottom', fontsize=10, color='#E74C3C',
            fontweight='bold')

    # Title
    ax.set_title('NCCL all_reduce_perf Results\np4d.24xlarge  |  8x A100-SXM4-40GB  |  NVSwitch',
                 fontsize=18, fontweight='bold', color=TEXT_COLOR, pad=20)

    # Axes
    ax.set_ylabel('Average Bus Bandwidth (GB/s)', fontsize=13, color=TEXT_COLOR)
    ax.set_xticks(x)
    ax.set_xticklabels(tests, fontsize=11, color=TEXT_COLOR)
    ax.set_ylim(0, 55)
    ax.yaxis.set_major_locator(plt.MultipleLocator(10))
    ax.grid(axis='y', alpha=0.3, color=GRID_COLOR, zorder=0)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#BDC3C7')
    ax.spines['bottom'].set_color('#BDC3C7')
    ax.tick_params(colors='#7F8C8D')

    # Legend
    same_patch = mpatches.Patch(color=BAR_COLOR_SAME, label='Same NUMA domain')
    cross_patch = mpatches.Patch(color=BAR_COLOR_CROSS, label='Cross NUMA domain')
    all_patch = mpatches.Patch(color=BAR_COLOR_ALL, label='All 8 GPUs')
    ax.legend(handles=[same_patch, cross_patch, all_patch],
              loc='upper left', fontsize=11, framealpha=0.9)

    # Key finding annotation
    # Compute max delta for 2-GPU tests
    delta_2gpu = abs(avg_bw[1] - avg_bw[0]) / avg_bw[0] * 100
    delta_4gpu = abs(avg_bw[3] - avg_bw[2]) / avg_bw[2] * 100

    bbox_props = dict(boxstyle="round,pad=0.5", facecolor='#FFF9C4',
                      edgecolor='#F9A825', alpha=0.95, linewidth=2)
    ax.text(0.98, 0.25,
            f'2-GPU: same vs cross NUMA = {delta_2gpu:.1f}% delta\n'
            f'4-GPU: same vs cross NUMA = {delta_4gpu:.1f}% delta\n\n'
            f'NVSwitch crossbar makes all placements\n'
            f'equivalent within a node.',
            transform=ax.transAxes, ha='right', va='center',
            fontsize=11, color=TEXT_COLOR, bbox=bbox_props, linespacing=1.5)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, 'gpu-nccl-benchmark.png')
    fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=BG_COLOR)
    plt.close(fig)
    print(f"  Created: {path}")


if __name__ == '__main__':
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Generating GPU topology diagrams...")
    generate_nvswitch_crossbar()
    generate_pcie_topology()
    generate_nccl_benchmark()
    print("Done. All diagrams saved to docs/diagrams/")
