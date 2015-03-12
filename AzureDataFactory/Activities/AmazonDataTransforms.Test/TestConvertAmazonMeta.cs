using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace AmazonDataTransforms.Test
{
    [TestClass]
    public class TestConvertAmazonMeta
    {
        [TestMethod]
        public void TestParseDiscontinued()
        {
            var meta = new AmazonMetadata();
            ConvertAmazonMeta.ParseProduct(Properties.Resources.Discontinued, meta);
            Assert.AreEqual("", meta.AsinProps.ToString());
        }

        [TestMethod]
        public void TestParseSimpleProduct()
        {
            var meta = new AmazonMetadata();
            ConvertAmazonMeta.ParseProduct(Properties.Resources.SimpleProduct, meta);
            Assert.AreEqual(meta.AsinProps.ToString(), "1,0827229534,Patterns of Preaching: A Sermon Sampler,Book,396585,2,5\r\n");
            Assert.AreEqual(meta.SimilarAsins.ToString(), "0827229534,0804215715\r\n0827229534,156101074X\r\n0827229534,0687023955\r\n0827229534,0687074231\r\n0827229534,082721619X\r\n");
            Assert.AreEqual(meta.Reviews.ToString(), "2000-07-28,A2JW67OY8U6HHK,5,10,9\r\n2003-12-14,A2VE83MZF98ITY,5,6,5\r\n");
            Assert.AreEqual(meta.AsinCategories.Count, 1);
            Assert.IsTrue(meta.AsinCategories["0827229534"].SetEquals(
                new HashSet<long>(new long[] { 283155, 1000, 22, 12290, 12360, 12368, 12370 })));
        }

        [TestMethod]
        public void TestParseNoReviews()
        {
            var meta = new AmazonMetadata();
            ConvertAmazonMeta.ParseProduct(Properties.Resources.NoReviews, meta);
            Assert.AreEqual(meta.AsinProps.ToString(), "5,1577943082,Prayers That Avail Much for Business: Executive,Book,455160,0,0\r\n");
            Assert.AreEqual(meta.SimilarAsins.ToString(), 
                "1577943082,157794349X\r\n"+
                "1577943082,0892749504\r\n" +
                "1577943082,1577941829\r\n" +
                "1577943082,0892749563\r\n" +
                "1577943082,1577946006\r\n");
            Assert.AreEqual(meta.Reviews.ToString(), "");
            Assert.AreEqual(meta.AsinCategories.Count, 1);
            Assert.IsTrue(meta.AsinCategories["1577943082"].SetEquals(
                new HashSet<long>(new long[] { 283155, 1000, 22, 12290, 12465, 12333, 12470, 297488 })));
        }

        [TestMethod]
        public void TestParseMultiWithHeader()
        {
            var meta = ConvertAmazonMeta.ConvertData(Properties.Resources.ProductsAndHeader);
            Assert.AreEqual(meta.AsinProps.ToString(), "1,0827229534,Patterns of Preaching: A Sermon Sampler,Book,396585,2,5\r\n");
            Assert.AreEqual(meta.SimilarAsins.ToString(), "0827229534,0804215715\r\n0827229534,156101074X\r\n0827229534,0687023955\r\n0827229534,0687074231\r\n0827229534,082721619X\r\n");
            Assert.AreEqual(meta.Reviews.ToString(), "2000-07-28,A2JW67OY8U6HHK,5,10,9\r\n2003-12-14,A2VE83MZF98ITY,5,6,5\r\n");
            Assert.AreEqual(meta.AsinCategories.Count, 1);
            Assert.IsTrue(meta.AsinCategories["0827229534"].SetEquals(
                new HashSet<long>(new long[] { 283155, 1000, 22, 12290, 12360, 12368, 12370 })));
        }
    }
}
