function decrementWithValidation(r)
    local res = 0
    if aerospike:exists(r) then
        if (r.count > 0 ) then
            res = r.count
            r.count = r.count -1
            aerospike:update(r)
        end
    end
    return res
end