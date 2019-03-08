package com.threathunter.greyhound.server.esper;

import com.threathunter.common.Utility;

import java.io.Serializable;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * EsperRule contains information for the statements run by Esper batch.
 * 
 * @author Wen Lu
 */
public class EsperEPL implements Serializable, Comparable<EsperEPL> {
	private static final long serialVersionUID = 1L;
    private static final Pattern pattern = Pattern.compile("(?<=@Priority\\()(\\d+?)(?=\\))");

	private String name;
	private String statement;
    private boolean started = true;
    private boolean needListen = false;
    private int priority = 0;

    // collector epl, but not useful, because in this case will probably not trigger
    // trigger is unidirectional
//    String rawJoinKey = genRawJoinKey(meta.getGroupKeys());
//        for (Identifier id : meta.getSrcVariableMetasID()) {
//        if (id.equals(meta.getTrigger())) {
//            continue;
//        }
//        String srcName = id.getKeys().get(1);
//        if (!wrapper.isInBatchQueryIds(id)) {
//            sb.append(" join ");
//            sb.append(srcName).append(".std:unique(").append(rawJoinKey).append(")").append(" as ").append(srcName).append(" on ")
//                    .append(triggerKeysExpression).append("=").append(srcName).append(".key");
//        }
//    }
//    private String genRawJoinKey(List<Property> joinKeys) {
//        StringBuilder sb = new StringBuilder();
//        boolean first = true;
//        for (Property p : joinKeys) {
//            if (first) {
//                first = false;
//            } else {
//                sb.append(", ");
//            }
//            sb.append(p.getName());
//        }
//        return sb.toString();
//    }

	public EsperEPL(String name, String statement, boolean started, boolean needListen,
                    int priority) {
		this.name = name;
		this.statement = statement;
        this.started = started;
        this.needListen = needListen;
        this.priority = priority;
        extractPriorityFromStatement(); // override the priority
	}

    public EsperEPL(String name, String statement, boolean started, boolean needListen) {
        this(name, statement, started, needListen, 0);
    }

    public EsperEPL(String name, String statement, boolean started ) {
        this(name, statement, started, true, 0);
    }

    public EsperEPL(String name, String statement) {
        this(name, statement, true, true, 0);
    }

    public EsperEPL(String name) {
        this(name, name, true, true, 0);
    }

    public EsperEPL() {
    }

    public String getName() {
		return name;
	}

    public void setName(String name) {
        this.name = name;
    }

    public String getStatement() {
		return statement;
	}

    public void setStatement(String statement) {
        this.statement = statement;
        extractPriorityFromStatement(); // override the priority
    }

    public boolean isStarted() {
        return started;
    }

    public void setStarted(boolean started) {
        this.started = started;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
        extractPriorityFromStatement(); // override the priority
    }

    public boolean isNeedListen() {
        return needListen;
    }

    public void setNeedListen(boolean needListen) {
        this.needListen = needListen;
    }

    private void extractPriorityFromStatement() {
        if (statement != null) {
            Matcher matcher = pattern.matcher(statement);
            if (matcher.find()) {
                try {
                    priority = Integer.parseInt(matcher.group());
                } catch(NumberFormatException nfe) {
                    throw new IllegalArgumentException("invalid priority", nfe);
                }
            }
        }
    }
    /**
	 * Only consider name and statement
	 */
	@Override
	public boolean equals(Object obj) {
        if (obj instanceof EsperEPL) {
            EsperEPL other = (EsperEPL)obj;
            if (Utility.isEqual(this.name, other.name) && Utility.isEqual(this.statement, other.statement)) {
                return true;
            }
        }
		return false;
	}

	@Override
	public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: ");
        sb.append(name);
        sb.append("\n");

        sb.append("statement: ");
        sb.append(statement);
        sb.append("\n");

        sb.append("needListen: ");
        sb.append(needListen);
        sb.append("\n");

        sb.append("priority: ");
        sb.append(priority);
        sb.append("\n");

        return sb.toString();
	}

	@Override
	public int hashCode() {
		int result;
		result = name == null ? 0 : name.hashCode();
		result = result * 31 + (statement == null ? 0 : statement.hashCode());
        return result;
	}

    @Override
    public int compareTo(EsperEPL o) {
        if (o == null) return 1;
        if (this.priority != o.priority) {
            return this.priority - o.priority;
        } else {
            return this.name.compareTo(o.name);
        }
    }

    public static Comparator<EsperEPL> comparatorASC = (o1, o2) -> o1.compareTo(o2);

    public static Comparator<EsperEPL> comparatorDESC = (o1, o2) -> o2.compareTo(o1);

}
