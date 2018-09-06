package io.axoniq.axonhub.serializer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class GsonMedia implements Media {

    private JsonObject jsonObject = new JsonObject();

    @Override
    public Media with(String property, String value) {
        jsonObject.addProperty(property, value);
        return this;
    }

    @Override
    public Media with(String property, Number value) {
        jsonObject.addProperty(property, value);
        return this;
    }

    @Override
    public Media with(String property, Boolean value) {
        jsonObject.addProperty(property, value);
        return this;
    }

    @Override
    public Media with(String property, Printable value) {
        jsonObject.add(property, print(value));
        return this;
    }

    @Override
    public Media with(String property, Iterable<? extends Printable> values) {
        JsonArray jsonArray = new JsonArray();
        values.forEach(value -> jsonArray.add(print(value)));
        jsonObject.add(property, jsonArray);
        return this;
    }

    @Override
    public Media withStrings(String property, Iterable<String> values) {
        JsonArray jsonArray = new JsonArray();
        values.forEach(jsonArray::add);
        jsonObject.add(property, jsonArray);
        return this;
    }


    private JsonObject print(Printable printable){
        GsonMedia jsonMediaTest = new GsonMedia();
        printable.printOn(jsonMediaTest);
        return jsonMediaTest.jsonObject;
    }

    @Override
    public String toString() {
        return new Gson().toJson(jsonObject);
    }
}
