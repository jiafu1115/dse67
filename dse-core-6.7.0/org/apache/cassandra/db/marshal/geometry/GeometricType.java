package org.apache.cassandra.db.marshal.geometry;

public enum GeometricType {
   POINT(Point.class, Point.serializer),
   LINESTRING(LineString.class, LineString.serializer),
   POLYGON(Polygon.class, Polygon.serializer);

   private final Class<? extends OgcGeometry> geoClass;
   private final OgcGeometry.Serializer serializer;

   private GeometricType(Class<? extends OgcGeometry> geoClass, OgcGeometry.Serializer serializer) {
      this.geoClass = geoClass;
      this.serializer = serializer;
   }

   public Class<? extends OgcGeometry> getGeoClass() {
      return this.geoClass;
   }

   public OgcGeometry.Serializer getSerializer() {
      return this.serializer;
   }
}
