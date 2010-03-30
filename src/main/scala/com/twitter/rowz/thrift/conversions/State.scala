package com.twitter.rowz.thrift.conversions


object State {
  class RichState(state: rowz.State.Value) {
    def toThrift = state.id
  }
  implicit def stateToRichState(state: rowz.State.Value) = new RichState(state)

  class RichInt(state: Int) {
    def fromThrift = rowz.State(state)
  }
  implicit def intToRichInt(state: Int) = new RichInt(state)
}
