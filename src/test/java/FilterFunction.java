import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.Arrays;

public class FilterFunction {
    

    public static void main(String[] args) throws ParseException {
        WKTReader wktReader = new WKTReader();
        String wkt= "LINESTRING(1 3,2 4)";
        Geometry geometry = wktReader.read(wkt);
        System.out.println(Arrays.toString(wkt.getBytes()));
        System.out.println(geometry.toText());
    }
}