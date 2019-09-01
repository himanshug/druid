package org.apache.druid.segment.vector;

import javax.annotation.Nullable;

public class DelegateVectorAdaptor implements VectorValueSelector
{
  protected final VectorObjectSelector objectSelector;

  protected DelegateVectorAdaptor(VectorObjectSelector objectSelector)
  {
    this.objectSelector = objectSelector;
  }

  @Override
  public long[] getLongVector()
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public float[] getFloatVector()
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public double[] getDoubleVector()
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Nullable
  @Override
  public boolean[] getNullVector()
  {
    return null;
  }

  @Override
  public int getMaxVectorSize()
  {
    return objectSelector.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return objectSelector.getCurrentVectorSize();
  }
}
