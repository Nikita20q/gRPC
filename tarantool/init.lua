box.cfg{
    listen = '3301',
    memtx_memory = 2 * 1024 * 1024 * 1024,
    wal_mode = 'write',
    readahead = 32 * 1024 * 1024,
    snapshot_count = 100000,
    snapshot_delay = 1
}

if box.space.KV == nil then
    local space = box.schema.space.create('KV')
    
    space:format({
        {name = 'key', type = 'string'},
        {name = 'value', type = 'varbinary', is_nullable = true}
    })
    
    space:create_index('primary', {
        type = 'tree',
        parts = {{field = 'key', type = 'string'}}
    })
    print("KV space created successfully.")
else
    print("KV space already exists.")
end
