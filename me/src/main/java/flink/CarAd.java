package flink;

import java.io.Serializable;

@SuppressWarnings("serial")
public final class CarAd implements Serializable {

    public String country,uniqueId,urlAnonymized,make,model,year,mileage,price,doors,fuel,carType,transmission,color,region,city,date,titleChunk,contentChunk;

    public CarAd() {}

    @Override public String toString() {
        return "CarAd [country=" + country + ", uniqueId=" + uniqueId + ", urlAnonymized=" + urlAnonymized + ", make="
                + make + ", model=" + model + ", year=" + year + ", mileage=" + mileage + ", price=" + price
                + ", doors=" + doors + ", fuel=" + fuel + ", carType=" + carType + ", transmission=" + transmission
                + ", color=" + color + ", region=" + region + ", city=" + city + ", date=" + date + ", titleChunk="
                + titleChunk + ", contentChunk=" + contentChunk + "]";
    }
}