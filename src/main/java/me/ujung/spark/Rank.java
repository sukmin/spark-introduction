package me.ujung.spark;

import java.io.Serializable;

/**
 * @author sukmin.kwon
 * @since 2017-02-07
 */
public class Rank implements Serializable {

	private ContentsArticle contentsArticle;

	public ContentsArticle getContentsArticle() {
		return contentsArticle;
	}

	public void setContentsArticle(ContentsArticle contentsArticle) {
		this.contentsArticle = contentsArticle;
	}

	@Override
	public String toString() {
		return "Rank{" +
			"contentsArticle=" + contentsArticle +
			'}';
	}

	public static class ContentsArticle implements Serializable {
		private String escapeTitle;
		private String articleId;

		public String getEscapeTitle() {
			return escapeTitle;
		}

		public void setEscapeTitle(String escapeTitle) {
			this.escapeTitle = escapeTitle;
		}

		public String getArticleId() {
			return articleId;
		}

		public void setArticleId(String articleId) {
			this.articleId = articleId;
		}

		@Override
		public String toString() {
			return "ContentsArticle{" +
				"escapeTitle='" + escapeTitle + '\'' +
				", articleId='" + articleId + '\'' +
				'}';
		}
	}

}
