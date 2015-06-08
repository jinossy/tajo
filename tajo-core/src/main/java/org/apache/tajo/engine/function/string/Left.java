/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.function.string;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text left(string text, int size)
 */
@Description(
  functionName = "left",
  description = "First n characters in the string.",
  example = "> SELECT left('ABC', 2);\n"
          + "AB",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT,
          TajoDataTypes.Type.INT4})}
)
public class Left extends GeneralFunction {
  public Left() {
    super(new Column[] {
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("size", TajoDataTypes.Type.INT4)
    });
  }

  public int getSize(int length, int size) {
    if (size < 0) {
        size = length + size;
        if (size < 0) {
            size = 0;
        }
    }

    return (size < length) ? size : length;
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    String data = params.getText(0);
    int length = data.length();
    int size = params.getInt4(1);

    size = getSize(length, size);
    if (size == 0) {
      return TextDatum.EMPTY_TEXT;
    }

    return DatumFactory.createText(data.substring(0, size));
  }
}
