package org.talend.sdk.component.api.component;

import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public interface InputFinder extends Serializable {

    Iterator<Record> find(final String emitterIdentifier,
            final int version,
            final Map<String, String> configuration);
}
