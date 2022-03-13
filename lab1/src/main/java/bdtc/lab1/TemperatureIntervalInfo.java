package bdtc.lab1;

import java.util.Objects;

public class TemperatureIntervalInfo {

    private final String name;
    private final Long start;
    private final Long end;

    public TemperatureIntervalInfo(String name, Long start, Long end) {
        this.name = name;
        this.start = start;
        this.end = end;
    }

    public String getName() {
        return name;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemperatureIntervalInfo that = (TemperatureIntervalInfo) o;
        return name.equals(that.name) && start.equals(that.start) && end.equals(that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, start, end);
    }

    @Override
    public String toString() {
        return "TemperatureIntervalInfo{" +
                "name='" + name + '\'' +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
