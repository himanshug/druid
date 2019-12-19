/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.security.pac4j;

import org.apache.commons.io.IOUtils;
import org.apache.druid.crypto.CryptoService;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Test;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.core.util.JavaSerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class HackTest
{
  @Test
  public void testme() throws Exception
  {

    String encoded = "AAAACAAAABAAAAeQ+ny2At0h/d5e/FzwMfsbCqtolecVoehVFODyn8AAzBT+qsXW2w48QYdGCQSo6Wuuj+j2vVaz5yOpOBqRCmkGqMr1kdpSo7zPPjY+q0sea+JbtK2bo5gF7WX7Ej5OuCH76PU3fHKQoIo10CoBozIsGmEwP+7YWNquAI5J6yYhsOumjgGJCO81mQM5ueMo/RMmPiIt5wYyH4zLtV/eiLVq41GImLI8pkMMIqzf8hxPEEVXha0HBR0PxfNIOez3lSf9vm2P826wT54RRx3pHAh3X67c7KZwlyxLrgSVDLky242bhO5TCBc4yfvpYsAQ77VWItwxFj/uxNdxhUhXgFubCvTTOgG4wMlbLs8yafxuQbgOALnLWPp8I87W3axZvwe1ZAyYg7KahnaioX4T08MiCid/TaE3zSryajcRO9pq0rHmFZiqItZ5b53A+ow56kwACVGUT1IzEIpVfv2+g8XpnPDE73JVzD6kbVZumMLrnzq27eqAG78Taqr6xUxCCXceuAxsZn7dIiHHkajxtVTubhMAvBhZ+Q9BwGi1y6fAYcEqb/fg9++lEoxb4lb5hd5jLpp5zz1sN+QbhurnDNjsx5rwiEVPWeQlFYSoXeX/3ITy51X/8cTO8Ipt8ldCvIznkuo3RcFBwiy67icroGclyxBRazHmASAlHmrbcJgP+V0TGKmKvXAXSZhBai3SoZK0VnThL4JaEjKttpOd9BtCMTR8pwhPfeUnrgS1g0rTYqAO0XlOaHNNbu8Ft84ES21IbRFO1WjDtDuWlw2IWg9w3K1PPC0eBF4YXfKgfPDZNpvn1+BvL414VoUty5TclgRXPn+1+xHMrhUEhbYO1F/AyRcc00x/xqayT6LNSAge3kRaW3UDrN7vFnUcTUCxQ5/Q3+D4ZbZpMXBXhMEQXWrW5aK1x7i6Vzu1TuUXF6HdOs54sH1keTOfGGOZsi8cICsLOcYX46QaPOnU8z9xdmIDvbG25ybOTvFp9NtJLaDNsj1oGxWtgLBmGsX8jBnF7XopC9J2TwwT87Nf0mVglHH96eXpXwS29E6TYXJRL1+0g09PQlkahaNNa9nxQNZdKrnBfyPt+I/KQZHoqq4JOvZX1ygFPyMYNLG7Gvxsp/78+RPIe8oFGVdCcz9vM3ojVt0cJQ4X4wb48o4ZurcPBKNjVAuWAylWJFJR1P3eLlCtFQ2wYGLFrCN5j9aX8HLT8mAC5/VcS4PsJsIuf9SDlYlAFSLi8NAL6FEOdC2QUrEJD+ag7tJlOCJfn/7/9pegxjqPByseC/fus5Wq2HrhDf7BzHHrVLjjb6NxgLxgyPi9jzj8N0y/ZXljEq2R53/FmeGkS29eB1KkpbPPbElau+6D5KQ77d49Z2PmFxIq8yFMzTRKjUpwOLFT7h1I0st8izjZPeSaTz7b4gnL7NkpUmxWsxVn+EymCQ7SGd6X6q2TdGjhQ87Rr3CzUB15icCx09Jog3tp8sm9Qu9GXbP2dnlepUkHltuuVf8e+bSDsBpPsa4/S1tkgIdyXbS4UTyc9QPJ8X5hmESlmbOOjsr89+nb5fWoMXLUZq7/pZMBln7yVF3w9aBQaX1VulqICCo5CjF9osJ6bOW+7Jt09rNgajJptULzeXAlu9ptW0MxCInJGAS/hcgyPi0CnjLzdnlquIz5ekC3loFMjs+C9BWYJQ6zpScBpY9W+nKvOZTq2s8YT8XF2TQ05S103uTgGWK+rEMabfSqPs4VYpa+S8Sg1QTKjk5JXfb6E0/rSogizNm5Q/3kzeqn4PWyPMZAul4OinnedaBN4YbYHLMveu4gc/vIX7CE+S7hqyiXek6dWA5b5V2qMSzOt3U4XEmLisxW6I/lqkJalkldBary901dem9nogVTb3QOOKc1HJ1wbyxGx51oxRr8fB/rqTXlk6YsI4p1yazlwkwMvcHs8ACSP/I8qF1PyWZJ8/uBmeRLoXgh1WLG+i8+2nJBgqlLV1LZWloR+d0aIhp2uU4JVsc8H4YQQxDIOgoEzzCuQK95EQGuk3vnVVVCcmtXxpOvTrb43M8ckiSN9/0DH1vyCvPp9DrvSEA6I8HIsjXln7xSvvWOroHcSOcT7y5KGXE3Fl2b75I+gJUuh//4OcTElNuj1EV3x4DdXbGyblc5z6o5Wj6CbNDFRUm4rGioyFXiyzKP959O5Xm4llle4XBP+dEWqoBJm32L3iwCeG3wwsWVvxUF4Dh+q3AHO8kIo2EbrRyaPRnYtpwA11TtYPdnTjSDboFIsfRUpdgtBYqYc2JoKwxF1iQx4ypgyw4gmiTNYVCtuBa+gUHqxFs46XPG24AW9BZDlnXLLW/8snEEHHo5K28DUmoxVHPO99h0hurAevj29oksXyvCE48SNe4yETvpPaddV44QWoJHNIvJX9VbxG5q2wq5dXgONWKDQDvnpvstLHvyR9NJI/vX5Tat8tQKjGm5JLoUFehcK2gxpP8XuX8+Z2IsQvEXkgxCme+D/rdl+/MHp9ZtCi4E85NuHLn6E5TTBKGP3xapNvx7t1/7uhAIHxA9wuvBvHw/92GzCfJ+RPDW0t3gLjtFXZzCmrgSyAU9RbdiplkAVxd2+5dtPKjP49gR3O3RV+tqXg==";
    JavaSerializationHelper javaSerializationHelper = new JavaSerializationHelper();

    Object obj = javaSerializationHelper.unserializeFromBytes(unCompress(new CryptoService(
        "himanshu",
        "AES",
        "PBKDF2WithHmacSHA1",
        "AES/CBC/PKCS5Padding",
        8,
        65536,
        128
    ).decrypt(StringUtils.decodeBase64String(encoded))));

    System.out.println(obj);


    /*
    OidcProfile fake = new OidcProfile();
    fake.setTokenExpirationAdvance(-1);
    fake.setId("welkgjasdfkaj");

    ((LinkedHashMap) obj).put("OidcClient", fake);
    */

    /*
    OidcProfile profile = (OidcProfile) ((LinkedHashMap) obj).get("OidcClient");
    profile.setIdTokenString("flkjejtqlewjfas");
    profile.setTokenExpirationAdvance(-1);
    */

    /*
    System.out.println(javaSerializationHelper.serializeToBase64((Serializable) obj));
    */

  }

  private byte[] unCompress(final byte[] data)
  {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
         GZIPInputStream gzip = new GZIPInputStream(inputStream)) {
      return IOUtils.toByteArray(gzip);
    }
    catch (IOException ex) {
      throw new TechnicalException(ex);
    }
  }
}
