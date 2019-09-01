package org.apache.druid.segment.vector;

import org.apache.druid.java.util.common.Numbers;

import javax.annotation.Nullable;

public class VectorObjectDoubleAdapter extends DelegateVectorAdaptor
{
  private final double nullValue;
  private final double[] doubles;

  public VectorObjectDoubleAdapter(VectorObjectSelector objectSelector, double nullValue)
  {
    super(objectSelector);
    doubles = new double[objectSelector.getMaxVectorSize()];
    this.nullValue = nullValue;
  }

  @Override
  public double[] getDoubleVector()
  {
    Object[] vector = objectSelector.getObjectVector();
    for (int i = 0; i < vector.length; i++) {
      doubles[i] = Numbers.tryParseDouble(vector[i], nullValue);
    }
    return doubles;
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    return null;
  }
}
