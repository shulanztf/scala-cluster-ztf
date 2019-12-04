package com.sutdy.meth

import java.io.File
import com.sutdy.model.HmUser
import com.sutdy.rich.RichHmUser

/**
 * 隐式转换PreDef
 */
class ZtfPreDef {

}

object ZtfPreDef {

  implicit def file2RichFile(f: File): RichFile = new RichFile(f)

  implicit def HmUser2RichHmUser(t: HmUser): RichHmUser = new RichHmUser(t)

}

