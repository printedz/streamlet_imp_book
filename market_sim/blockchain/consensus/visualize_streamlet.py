#!/usr/bin/env python3
"""
Streamlet Protocol Visualization

Generates an HTML visualization of the Streamlet protocol execution,
showing:
1. Blockchain growth over epochs (block tree with notarization/finalization)
2. Leader distribution (honest vs corrupt)
3. Notarization and finalization progress over time
4. Consistency verification across nodes

Usage:
    python visualize_streamlet.py [--nodes N] [--corrupt F] [--epochs E] [--output FILE]
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from blockchain.consensus.streamlet import StreamletProtocol, NodeType


def run_simulation(n_nodes: int, n_corrupt: int, n_epochs: int) -> dict:
    """Run the Streamlet protocol and collect visualization data."""
    protocol = StreamletProtocol(
        n_nodes=n_nodes,
        n_corrupt=n_corrupt,
        epoch_duration=1.0,
        network_delay=0.05,
        max_network_delay=0.2,
    )

    # Generate some sample transactions
    transactions = [
        [
            {
                "type": "trade",
                "symbol": "AAPL",
                "price": 150.0 + e * 0.5,
                "qty": 10,
                "epoch": e,
            }
        ]
        for e in range(n_epochs)
    ]

    results = protocol.run(n_epochs=n_epochs, transactions=transactions)

    # Collect block tree data from node 0
    node0 = protocol.nodes[0]
    blocks_data = []
    for bh, block in node0.state.blocks.items():
        is_notarized = node0.state.is_notarized(bh)
        is_finalized = bh in {b.block_hash for b in node0.state.finalized_log}
        blocks_data.append(
            {
                "hash": bh[:12],
                "epoch": block.epoch,
                "parent_hash": block.parent_hash[:12] if block.parent_hash else None,
                "notarized": is_notarized,
                "finalized": is_finalized,
                "tx_count": len(block.transactions),
            }
        )

    # Per-node finalized log lengths
    node_logs = []
    for node in protocol.nodes:
        node_logs.append(
            {
                "node_id": node.node_id,
                "type": node.node_type.value,
                "finalized_length": len(node.state.finalized_log),
                "finalized_epochs": [
                    b.epoch for b in node.state.finalized_log
                ],
            }
        )

    return {
        "params": {
            "n_nodes": n_nodes,
            "n_corrupt": n_corrupt,
            "n_epochs": n_epochs,
        },
        "metrics": results["metrics"],
        "blocks": blocks_data,
        "node_logs": node_logs,
        "consistency": results["consistency_check"],
    }


def generate_html(data: dict) -> str:
    """Generate a self-contained HTML visualization."""
    params = data["params"]
    metrics = data["metrics"]
    blocks = data["blocks"]
    node_logs = data["node_logs"]

    # Compute cumulative notarizations and finalizations per epoch
    notarization_cumulative = []
    finalization_cumulative = []
    n_count = 0
    f_count = 0
    notarized_epochs_set = {n["epoch"] for n in metrics["notarization_history"]}
    finalized_epochs_set = {f["epoch"] for f in metrics["finalization_history"]}

    for e in range(1, params["n_epochs"] + 1):
        if e in notarized_epochs_set:
            n_count += 1
        if e in finalized_epochs_set:
            f_count += 1
        notarization_cumulative.append(n_count)
        finalization_cumulative.append(f_count)

    # Leader data
    leader_colors = []
    for is_honest in metrics["leader_is_honest"]:
        leader_colors.append("#4CAF50" if is_honest else "#F44336")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Streamlet Protocol Visualization</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: 'Segoe UI', system-ui, sans-serif;
        background: #0f1923;
        color: #e0e0e0;
        padding: 20px;
    }}
    h1 {{
        text-align: center;
        color: #64B5F6;
        margin-bottom: 8px;
        font-size: 1.8em;
    }}
    .subtitle {{
        text-align: center;
        color: #90A4AE;
        margin-bottom: 24px;
        font-size: 0.95em;
    }}
    .grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 20px;
        max-width: 1400px;
        margin: 0 auto;
    }}
    .card {{
        background: #1a2736;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #263545;
    }}
    .card h2 {{
        color: #64B5F6;
        margin-bottom: 16px;
        font-size: 1.1em;
    }}
    .full-width {{ grid-column: 1 / -1; }}
    .stats {{
        display: flex;
        gap: 16px;
        flex-wrap: wrap;
        justify-content: center;
    }}
    .stat {{
        background: #223344;
        border-radius: 8px;
        padding: 12px 20px;
        text-align: center;
        min-width: 120px;
    }}
    .stat .value {{
        font-size: 1.8em;
        font-weight: bold;
        color: #64B5F6;
    }}
    .stat .label {{
        font-size: 0.8em;
        color: #90A4AE;
        margin-top: 4px;
    }}
    .consistency-badge {{
        display: inline-block;
        padding: 6px 16px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 0.9em;
    }}
    .consistent {{ background: #1B5E20; color: #A5D6A7; }}
    .inconsistent {{ background: #B71C1C; color: #EF9A9A; }}
    canvas {{ max-height: 300px; }}
    .block-tree {{
        overflow-x: auto;
        padding: 10px 0;
    }}
    .block-row {{
        display: flex;
        align-items: center;
        gap: 4px;
        margin: 4px 0;
        font-size: 0.75em;
        font-family: monospace;
    }}
    .block-cell {{
        width: 36px;
        height: 28px;
        display: flex;
        align-items: center;
        justify-content: center;
        border-radius: 4px;
        font-size: 0.75em;
        flex-shrink: 0;
    }}
    .block-empty {{ background: transparent; }}
    .block-proposed {{ background: #37474F; border: 1px solid #546E7A; }}
    .block-notarized {{ background: #1565C0; border: 1px solid #42A5F5; }}
    .block-finalized {{ background: #2E7D32; border: 1px solid #66BB6A; }}
    .node-row {{
        display: flex;
        align-items: center;
        gap: 8px;
        margin: 6px 0;
    }}
    .node-label {{
        width: 80px;
        font-size: 0.8em;
        color: #90A4AE;
    }}
    .node-bar {{
        height: 20px;
        border-radius: 4px;
        display: flex;
        align-items: center;
        padding: 0 8px;
        font-size: 0.7em;
        color: white;
    }}
    .legend {{
        display: flex;
        gap: 16px;
        margin-top: 10px;
        font-size: 0.8em;
    }}
    .legend-item {{
        display: flex;
        align-items: center;
        gap: 4px;
    }}
    .legend-color {{
        width: 12px;
        height: 12px;
        border-radius: 3px;
    }}
</style>
</head>
<body>

<h1>Streamlet Consensus Protocol</h1>
<p class="subtitle">
    {params['n_nodes']} nodes ({params['n_corrupt']} corrupt) &middot;
    {params['n_epochs']} epochs &middot;
    Consistency:
    <span class="consistency-badge {'consistent' if data['consistency'] else 'inconsistent'}">
        {'CONSISTENT' if data['consistency'] else 'INCONSISTENT'}
    </span>
</p>

<div class="grid">

    <!-- Summary Stats -->
    <div class="card full-width">
        <h2>Protocol Summary</h2>
        <div class="stats">
            <div class="stat">
                <div class="value">{metrics['blocks_proposed']}</div>
                <div class="label">Blocks Proposed</div>
            </div>
            <div class="stat">
                <div class="value">{metrics['blocks_notarized']}</div>
                <div class="label">Blocks Notarized</div>
            </div>
            <div class="stat">
                <div class="value">{metrics['blocks_finalized']}</div>
                <div class="label">Blocks Finalized</div>
            </div>
            <div class="stat">
                <div class="value">{metrics['votes_cast']}</div>
                <div class="label">Votes Cast</div>
            </div>
            <div class="stat">
                <div class="value">{sum(metrics['leader_is_honest'])}/{params['n_epochs']}</div>
                <div class="label">Honest Leaders</div>
            </div>
        </div>
    </div>

    <!-- Notarization & Finalization Progress -->
    <div class="card">
        <h2>Notarization &amp; Finalization Progress</h2>
        <canvas id="progressChart"></canvas>
    </div>

    <!-- Leader Distribution -->
    <div class="card">
        <h2>Epoch Leader Distribution</h2>
        <canvas id="leaderChart"></canvas>
    </div>

    <!-- Block Timeline -->
    <div class="card full-width">
        <h2>Block Timeline (Node 0 View)</h2>
        <div class="block-tree" id="blockTree"></div>
        <div class="legend">
            <div class="legend-item">
                <div class="legend-color" style="background:#37474F;border:1px solid #546E7A;"></div>
                Proposed
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background:#1565C0;border:1px solid #42A5F5;"></div>
                Notarized
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background:#2E7D32;border:1px solid #66BB6A;"></div>
                Finalized
            </div>
        </div>
    </div>

    <!-- Per-Node Finalized Logs -->
    <div class="card full-width">
        <h2>Finalized Log Per Node</h2>
        <div id="nodeLogs"></div>
    </div>

</div>

<script>
const data = {json.dumps(data)};
const notarizationCumulative = {json.dumps(notarization_cumulative)};
const finalizationCumulative = {json.dumps(finalization_cumulative)};
const leaderColors = {json.dumps(leader_colors)};
const epochs = Array.from({{length: data.params.n_epochs}}, (_, i) => i + 1);

// Progress Chart
new Chart(document.getElementById('progressChart'), {{
    type: 'line',
    data: {{
        labels: epochs,
        datasets: [
            {{
                label: 'Cumulative Notarized',
                data: notarizationCumulative,
                borderColor: '#42A5F5',
                backgroundColor: 'rgba(66,165,245,0.1)',
                fill: true,
                tension: 0.3,
            }},
            {{
                label: 'Cumulative Finalized',
                data: finalizationCumulative,
                borderColor: '#66BB6A',
                backgroundColor: 'rgba(102,187,106,0.1)',
                fill: true,
                tension: 0.3,
            }}
        ]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{ labels: {{ color: '#B0BEC5' }} }}
        }},
        scales: {{
            x: {{
                title: {{ display: true, text: 'Epoch', color: '#B0BEC5' }},
                ticks: {{ color: '#78909C' }},
                grid: {{ color: '#263545' }}
            }},
            y: {{
                title: {{ display: true, text: 'Count', color: '#B0BEC5' }},
                ticks: {{ color: '#78909C' }},
                grid: {{ color: '#263545' }}
            }}
        }}
    }}
}});

// Leader Chart
new Chart(document.getElementById('leaderChart'), {{
    type: 'bar',
    data: {{
        labels: epochs,
        datasets: [{{
            label: 'Leader Node',
            data: data.metrics.epoch_leaders,
            backgroundColor: leaderColors,
            borderRadius: 2,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{ display: false }},
            tooltip: {{
                callbacks: {{
                    label: function(ctx) {{
                        const honest = data.metrics.leader_is_honest[ctx.dataIndex];
                        return `Node ${{ctx.raw}} (${{honest ? 'honest' : 'CORRUPT'}})`;
                    }}
                }}
            }}
        }},
        scales: {{
            x: {{
                title: {{ display: true, text: 'Epoch', color: '#B0BEC5' }},
                ticks: {{ color: '#78909C' }},
                grid: {{ color: '#263545' }}
            }},
            y: {{
                title: {{ display: true, text: 'Node ID', color: '#B0BEC5' }},
                ticks: {{ color: '#78909C', stepSize: 1 }},
                grid: {{ color: '#263545' }}
            }}
        }}
    }}
}});

// Block Timeline
const blockTree = document.getElementById('blockTree');
const maxEpoch = data.params.n_epochs;
let html = '<div class="block-row"><span style="width:36px;text-align:center;font-size:0.7em;color:#546E7A;">Epoch</span>';
for (let e = 0; e <= maxEpoch; e++) {{
    html += `<div class="block-cell block-empty" style="color:#546E7A;font-size:0.65em;">${{e}}</div>`;
}}
html += '</div>';

// Build block row
html += '<div class="block-row"><span style="width:36px;text-align:center;font-size:0.7em;color:#90A4AE;">Chain</span>';
const blocksByEpoch = {{}};
data.blocks.forEach(b => {{ blocksByEpoch[b.epoch] = b; }});

for (let e = 0; e <= maxEpoch; e++) {{
    const b = blocksByEpoch[e];
    if (b) {{
        let cls = 'block-proposed';
        if (b.finalized) cls = 'block-finalized';
        else if (b.notarized) cls = 'block-notarized';
        html += `<div class="block-cell ${{cls}}" title="Epoch ${{b.epoch}}\\nHash: ${{b.hash}}\\nTxs: ${{b.tx_count}}">${{b.epoch}}</div>`;
    }} else {{
        html += '<div class="block-cell block-empty"></div>';
    }}
}}
html += '</div>';
blockTree.innerHTML = html;

// Node Logs
const nodeLogs = document.getElementById('nodeLogs');
const maxLog = Math.max(...data.node_logs.map(n => n.finalized_length), 1);
let logsHtml = '';
data.node_logs.forEach(n => {{
    const width = (n.finalized_length / maxLog) * 100;
    const color = n.type === 'honest' ? '#1565C0' : '#C62828';
    const label = n.type === 'honest' ? 'H' : 'C';
    logsHtml += `
        <div class="node-row">
            <span class="node-label">Node ${{n.node_id}} (${{label}})</span>
            <div class="node-bar" style="width:${{Math.max(width, 5)}}%;background:${{color}};">
                ${{n.finalized_length}} blocks: [${{n.finalized_epochs.join(', ')}}]
            </div>
        </div>`;
}});
nodeLogs.innerHTML = logsHtml;
</script>

</body>
</html>"""
    return html


def main():
    parser = argparse.ArgumentParser(
        description="Visualize Streamlet protocol execution"
    )
    parser.add_argument("--nodes", type=int, default=10, help="Total nodes")
    parser.add_argument("--corrupt", type=int, default=3, help="Corrupt nodes")
    parser.add_argument("--epochs", type=int, default=30, help="Epochs to run")
    parser.add_argument(
        "--output", type=str, default=None, help="Output HTML file"
    )
    args = parser.parse_args()

    print(f"Running Streamlet: {args.nodes} nodes, {args.corrupt} corrupt, {args.epochs} epochs...")
    data = run_simulation(args.nodes, args.corrupt, args.epochs)

    html = generate_html(data)

    output_path = args.output or os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "streamlet_visualization.html"
    )
    output_path = os.path.abspath(output_path)

    with open(output_path, "w") as f:
        f.write(html)

    print(f"Visualization saved to: {output_path}")
    print(f"  Proposed: {data['metrics']['blocks_proposed']}")
    print(f"  Notarized: {data['metrics']['blocks_notarized']}")
    print(f"  Finalized: {data['metrics']['blocks_finalized']}")
    print(f"  Consistency: {'PASS' if data['consistency'] else 'FAIL'}")


if __name__ == "__main__":
    main()
