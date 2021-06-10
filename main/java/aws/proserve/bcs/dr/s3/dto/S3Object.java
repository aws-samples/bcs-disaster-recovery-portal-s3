// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aws.proserve.bcs.dr.s3.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableS3Object.class)
@JsonDeserialize(as = ImmutableS3Object.class)
@Value.Immutable
public interface S3Object {
    long COMPLETED_SIZE = -1L;
    String COMPLETED_KEY = "DRPS3-FinalMarker";

    @JsonIgnore
    default boolean isCompleted() {
        return COMPLETED_KEY.equals(getKey()) && COMPLETED_SIZE == getSize();
    }

    String getKey();

    long getSize();
}
