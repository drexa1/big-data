package com.druizbarbero.sensors.data;

import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class SingleSensorDataPoint extends AbstractDataPoint {

    /**
     * The name of the sensor from which the measure is coming.
     */
    @NonNull private String sensor;

    /**
     * The anomaly likelihood after having been tagged by the scoring function.
     */
    private Float score;

}
