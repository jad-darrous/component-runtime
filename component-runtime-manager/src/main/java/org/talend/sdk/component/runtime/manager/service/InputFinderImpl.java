package org.talend.sdk.component.runtime.manager.service;

import lombok.RequiredArgsConstructor;
import org.talend.sdk.component.api.component.InputFinder;
import org.talend.sdk.component.api.component.MigrationHandler;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.record.RecordConverters;
import org.talend.sdk.component.runtime.serialization.SerializableService;

import java.io.ObjectStreamException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

@RequiredArgsConstructor
public class InputFinderImpl implements InputFinder {

    private final String plugin;

    private final Function<String, ComponentFamilyMeta.BaseMeta<Mapper>> mapperFinder;

    private final Function<Object, Record> recordConverter;

    @Override
    public Iterator<Record> find(String emitterIdentifier,
            int version,
            Map<String, String> configuration) {

        final Mapper mapper = this.findMapper(emitterIdentifier, version, configuration);
        final Input input = mapper.create();
        final Iterator<Object> iteratorObject = new InputIterator(input);

        return new IteratorMap<>(iteratorObject, this.recordConverter);
    }

    static class InputIterator implements Iterator<Object> {

        private final Input input;

        private Object nextObject;

        InputIterator(Input input) {
            this.input = input;
            this.nextObject = InputIterator.findNext(input);
        }

        @Override
        public boolean hasNext() {
            return this.nextObject != null;
        }

        @Override
        public Object next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            final Object current = this.nextObject;
            this.nextObject = InputIterator.findNext(input);
            return current;
        }

        private static Object findNext(final Input input) {
            return input.next();
        }
    }

    @RequiredArgsConstructor
    static class IteratorMap<T, U> implements Iterator<U> {

        private final Iterator<T> wrappedIterator;

        private final Function<T, U> converter;

        @Override
        public boolean hasNext() {
            return this.wrappedIterator.hasNext();
        }

        @Override
        public U next() {
            final T next = this.wrappedIterator.next();
            return this.converter.apply(next);
        }
    }

    private Mapper findMapper(String emitterIdentifier,
            int version,
            Map<String, String> configuration) {
        final ComponentFamilyMeta.BaseMeta<Mapper> meta = this.mapperFinder.apply(emitterIdentifier);
        if (configuration == null) {
            return meta.getInstantiator().apply(null);
        }
        final Supplier<MigrationHandler> migrationHandler = meta.getMigrationHandler();
        final Map<String, String> migratedConfiguration = migrationHandler.get().migrate(version, configuration);
        return meta.getInstantiator().apply(migratedConfiguration);
    }

    private Object writeReplace() throws ObjectStreamException {
        return new SerializableService(plugin, InputFinder.class.getName());
    }
}
