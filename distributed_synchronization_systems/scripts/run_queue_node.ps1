param(
    [Parameter(Mandatory=$true)]
    [int]$port
)

cd ..

./venv/scripts/activate

python -m src.nodes.queue_node --port $port