package nta.engine.function;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import nta.catalog.Column;
import nta.datum.Datum;
import nta.engine.json.GsonCreator;
import nta.engine.utils.TUtil;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public abstract class Function<T extends Datum> implements Cloneable {
  @Expose protected Column[] definedParams;
  public final static Column [] NoArgs = new Column [] {};

  public Function(Column[] definedArgs) {
    this.definedParams = definedArgs;
  }

  public abstract void init();

  public abstract void eval(Tuple params);

  public abstract void merge(Tuple...params);

  public abstract T terminate();

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GeneralFunction) {
      GeneralFunction other = (GeneralFunction) obj;
      return TUtil.checkEquals(definedParams, other.definedParams);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(definedParams);
  }

  public Object clone() throws CloneNotSupportedException {
    GeneralFunction func = (GeneralFunction) super.clone();
    func.definedParams = definedParams != null ? definedParams.clone() : null;
    return func;
  }

  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, Function.class);
  }
}
