param(
    [Parameter(Mandatory=$true)]
    [int]$port
)

cd ..

./venv/scripts/activate

python -m src.nodes.lock_manager --port $port