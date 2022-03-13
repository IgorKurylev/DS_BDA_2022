package bdtc.lab1;

import java.awt.*;
import java.util.Objects;


/**
 * Data-class координат зоны для сериалиации в JSON
 */
public class ZoneInfo {
    private final Point startPoint;
    private final Point endPoint;
    private final String name;

    public ZoneInfo(String name, Point startCoords, Point endCoords) {
        super();
        this.name = name;
        this.startPoint = startCoords;
        this.endPoint = endCoords;
    }

    public String getName() {
        return name;
    }

    public Point getStartPoint() {
        return startPoint;
    }

    public Point getEndPoint() {
        return endPoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZoneInfo zoneInfo = (ZoneInfo) o;
        return startPoint.equals(zoneInfo.startPoint) && endPoint.equals(zoneInfo.endPoint) && name.equals(zoneInfo.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startPoint, endPoint, name);
    }

    @Override
    public String toString() {
        return "ZoneInfo{" +
                "startPoint=" + startPoint +
                ", endPoint=" + endPoint +
                ", name='" + name + '\'' +
                '}';
    }
}