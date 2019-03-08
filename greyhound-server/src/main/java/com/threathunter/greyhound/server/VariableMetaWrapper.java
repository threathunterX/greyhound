package com.threathunter.greyhound.server;

import com.threathunter.common.Identifier;
import com.threathunter.model.VariableMeta;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by daisy on 17-11-12
 */
public class VariableMetaWrapper<V extends VariableMeta> {
    private final V meta;

    private final List<Identifier> batchQueryIds;

    public VariableMetaWrapper(V meta, List<Identifier> batchQueryIds) {
        this.meta = meta;
        this.batchQueryIds = batchQueryIds;
    }

    public V getMeta() {
        return meta;
    }

    public boolean isInBatchQueryIds(Identifier id) {
        if (batchQueryIds == null || batchQueryIds.isEmpty()) {
            return false;
        }

        for (Identifier bid : batchQueryIds) {
            if (bid.equals(id)) {
                return true;
            }
        }

        return false;
    }

    public List<Identifier> getBatchQueryIds() {
        return this.batchQueryIds;
    }

    public static List<VariableMetaWrapper> generateWrappers(List<VariableMeta> metas, Set<String> batchSlotDimensions) {
        List<VariableMetaWrapper> result = new ArrayList<>();
        if (batchSlotDimensions == null || batchSlotDimensions.isEmpty()) {
            metas.forEach(meta -> result.add(new VariableMetaWrapper(meta, null)));
        } else {
            Set<Identifier> batchQueryIds = new HashSet<>();
            metas.forEach(meta -> {
                if (!meta.getDimension().isEmpty()) {
                    if (batchSlotDimensions.contains(meta.getDimension())) {
                        if (!meta.getName().contains("trigger") && !meta.getName().contains("collect")) {
                            batchQueryIds.add(meta.getId());
                        }
                    }
                }
            });

            // if sorted, we may only need iterate once
            metas.forEach(meta -> {
                ArrayList<Identifier> mBatchIds = new ArrayList<>();
                meta.getSrcVariableMetasID().forEach(id -> {
                    if (batchQueryIds.contains(id)) {
                        mBatchIds.add(id);
                    }
                });

                result.add(new VariableMetaWrapper(meta, mBatchIds));
            });
        }

        return result;
    }
}
