package com.threathunter.greyhound.server.utils;

import com.threathunter.model.VariableMetaRegistry;
import com.threathunter.util.SystemClock;
import com.threathunter.variable.meta.CollectorVariableMeta;
import com.threathunter.variable.meta.DelayCollectorVariableMeta;

import java.util.*;

/**
 * Created by daisy on 16/7/20.
 */
public class StrategyInfoCache {
    private static final StrategyInfoCache INSTANCE = new StrategyInfoCache();
    public static StrategyInfoCache getInstance() {
        return INSTANCE;
    }

    private static final Map<String, Integer> priorMap = new HashMap<>();

    private volatile Map<String, StrategyInfo> cache;
    private volatile long lastUpdateTime = -1;

    static {
        priorMap.put("OTHER", 0);
        priorMap.put("VISITOR", 1);
        priorMap.put("ACCOUNT", 2);
        priorMap.put("MARKETING", 3);
        priorMap.put("ORDER", 4);
        priorMap.put("TRANSACTION", 5);
    }

    public void update(List<Map<String, Object>> strategies) {
        Map<String, StrategyInfo> newCache = new HashMap<>();
        for (Map<String, Object> strategy : strategies) {
            StrategyInfo info = new StrategyInfo();
            info.setCategory((String) strategy.get("category"));
            info.setScore(((Number) strategy.get("score")).longValue());
            info.setTags(new HashSet<>((List<String>) strategy.get("tags")));
            info.setTest((Boolean) strategy.getOrDefault("test", false));
            info.setCheckValue((String) strategy.getOrDefault("checkvalue", "c_ip"));
            info.setProfileScope((strategy.get("scope")).equals("profile"));
            info.setCheckType((String) strategy.getOrDefault("checktype", ""));
            info.setCheckPoints((String) strategy.getOrDefault("checkpoints", ""));
            info.setDecision((String) strategy.getOrDefault("decision", ""));
            info.setExpire(((Number) strategy.get("expire")).longValue());
            info.setTtl(((Number) strategy.getOrDefault("ttl", 300)).longValue());
            info.setRemark((String) strategy.getOrDefault("remark", ""));
            newCache.put((String) strategy.get("name"), info);
        }

        VariableMetaRegistry.getInstance().getAllVariableMetas().stream()
                .filter(meta -> meta.getName().contains("collect"))
                .forEach(meta -> {
                    CollectorVariableMeta cMeta = (CollectorVariableMeta) meta;
                    StrategyInfo info = newCache.get(cMeta.getStrategyName());
                    boolean isDelay = false;
                    if (cMeta instanceof DelayCollectorVariableMeta) {
                        info.setDelayMillis(((DelayCollectorVariableMeta) cMeta).getSleepTimeMillis());
                        isDelay = true;
                    }
                    if (info != null) {
                        info.addDimensionStrategy(String.format("%s@@%s", cMeta.getStrategyName(), cMeta.getDimension()), isDelay);
                    }
                });

        cache = newCache;
        lastUpdateTime = SystemClock.getCurrentTimestamp();
    }

    public boolean containsStrategy(String strategy) {
        return this.cache.containsKey(strategy);
    }

    public String getCategory(String strategy) {
        return this.cache.get(strategy).getCategory();
    }

    public Long getScore(String strategy) {
        return this.cache.get(strategy).getScore();
    }

    public Set<String> getTags(String strategy) {
        return this.cache.get(strategy).getTags();
    }

    public Boolean isTest(String strategy) {
        return this.cache.get(strategy).isTest();
    }

    public Boolean isProfileScope(String strategy) {
        return this.cache.get(strategy).isProfileScope();
    }

    public String getPriorCategory(String category1, String category2) {
        if (priorMap.containsKey(category1) && priorMap.containsKey(category2)) {
            return priorMap.get(category1) - priorMap.get(category2) > 0 ? category1 : category2;
        } else {
            if (priorMap.containsKey(category1)) {
                return category1;
            } else {
                return category2;
            }
        }
    }

    public StrategyInfo getStrategyInfo(String strategy) {
        return this.cache.get(strategy);
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public static class StrategyInfo {
        private Set<String> tags = new HashSet<>();
        private Long score;
        private String category;
        private Boolean test;
        private String checkType;
        private Boolean profileScope;
        private Long expire;
        private Long ttl;
        private String checkValue;
        private String decision;
        private String checkPoints;
        private String remark;
        private long delayMillis = 0;

        private List<String> allDimensionedExpression = new ArrayList<>(3);
        private List<String> allDelayDimensionedExpression = new ArrayList<>(3);

        public List<String> getAllDimensionedExpression(boolean forDelay) {
            if (forDelay) {
                return allDelayDimensionedExpression;
            }
            return allDimensionedExpression;
        }

        public void addDimensionStrategy(String dimensionStrategy, boolean isDelay) {
            this.allDimensionedExpression.add(dimensionStrategy);
            if (isDelay) {
                this.allDelayDimensionedExpression.add(String.format("%s@@%s", dimensionStrategy, "delay"));
            }
        }

        public Long getExpire() {
            return expire;
        }

        public void setExpire(Long expire) {
            this.expire = expire;
        }

        public String getCheckValue() {
            return checkValue;
        }

        public void setCheckValue(String checkValue) {
            this.checkValue = checkValue;
        }

        public String getDecision() {
            return decision;
        }

        public void setDecision(String decision) {
            this.decision = decision;
        }

        public String getCheckPoints() {
            return checkPoints;
        }

        public void setCheckPoints(String checkPoints) {
            this.checkPoints = checkPoints;
        }

        public String getRemark() {
            return remark;
        }

        public void setRemark(String remark) {
            this.remark = remark;
        }

        public Boolean isProfileScope() {
            return profileScope;
        }

        public void setProfileScope(Boolean profileScope) {
            this.profileScope = profileScope;
        }

        public String getCheckType() {
            return checkType;
        }

        public void setCheckType(String checkType) {
            this.checkType = checkType;
        }

        public Boolean isTest() {
            return test;
        }

        public void setTest(Boolean test) {
            this.test = test;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTags(Set<String> tags) {
            if (tags != null) {
                this.tags = tags;
            }
        }

        public Long getScore() {
            return score;
        }

        public void setScore(Long score) {
            this.score = score;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public Long getTtl() {
            return ttl;
        }

        public void setTtl(Long ttl) {
            this.ttl = ttl;
        }

        public long getDelayMillis() {
            return delayMillis;
        }

        public void setDelayMillis(long delayMillis) {
            this.delayMillis = delayMillis;
        }
    }
}
